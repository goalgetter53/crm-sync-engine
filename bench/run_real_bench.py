"""Benchmark on REAL company data: Wikidata companies (CC0) and their real, dated history.

Data
  bench/data/wikidata_companies.csv       companies (label, website, country, industry, HQ,
                                          inception, dissolution) from query.wikidata.org
  bench/data/wikidata_company_facts.csv   every dated (P585) employee count (P1128) and
                                          USD revenue (P2139) statement of those companies
  Re-download with --refresh (raw SPARQL JSON is cached in data/real/, which is gitignored).

Replay
  The warehouse `companies` table is rebuilt "as of" a date from that history: a company exists
  from its inception to its dissolution, and carries the latest headcount / revenue published by
  then. Initial full sync as of 2015-12-31, then one incremental sync per year-end 2016..2025 and
  a last one as of the retrieval date. Between runs the warehouse gets exactly the real delta
  (new companies, dissolved companies, changed headcount / revenue).

Scenarios
  engine       the replay against a fake HubSpot that enforces Free/Starter limits (100 req/10 s,
               5 searches/s) AND injects faults: 429s, 5xx, committed-then-502, timeouts
               (committed, response held past the client timeout) and 207 partial batch
               failures. Then faults off, converge, verify every row, re-run for idempotence.
  unthrottled  the same replay with no limits and no faults (raw engine throughput).
  naive        one request per record (GET by unique key, then POST or PATCH; GET + DELETE for
               removals), handed the exact delta for free, no batching, no limits.

Everything runs on one machine against the fake server on localhost; API-call counts transfer
to HubSpot, wall-clock times do not. No contacts or other personal data are involved.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Engine,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    insert,
    select,
    update,
)

from crm_sync.clock import SystemClock
from crm_sync.datasets import wikidata as wd
from crm_sync.demo import utcnow
from crm_sync.dsl import compile_mapping, load_mapping
from crm_sync.dsl.loader import CompiledMapping
from crm_sync.engine import SyncEngine
from fake_hubspot import FakeHub, FaultConfig
from run_bench import Server, run_sync

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
DATA = ROOT / "bench" / "data"
CACHE = ROOT / "data" / "real"
MAPPING = ROOT / "examples" / "mapping_real_companies.yaml"
INITIAL = date(2015, 12, 31)
HUBSPOT_PER_10S = 100  # Free/Starter private app burst limit
HUBSPOT_SEARCH_PER_S = 5
CLIENT_TIMEOUT_S = 1.0
FAULTS = {
    "p_429": 0.02,
    "p_5xx": 0.01,
    "p_apply_then_5xx": 0.01,
    "p_timeout": 0.01,
    "timeout_delay_s": 2.0,
    "p_partial_batch": 0.05,
}
NO_FAULTS = dict.fromkeys(FAULTS, 0.0)

metadata = MetaData()
companies = Table(
    "companies",
    metadata,
    Column("qid", String(20), primary_key=True),
    Column("name", String(400)),
    Column("domain", String(255)),
    Column("website", String(1000)),
    Column("country", String(200)),
    Column("hq_city", String(200)),
    Column("industry", String(200)),
    Column("founded_year", String(4)),
    Column("employees", BigInteger),
    Column("revenue_usd", BigInteger),
    Column("updated_at", DateTime),
)


# -- data -------------------------------------------------------------------------------------
def load_data(refresh: bool) -> tuple[dict[str, wd.Company], list[wd.Fact], dict[str, Any]]:
    source = DATA / "wikidata_source.json"
    if refresh or not source.exists() or not (DATA / "wikidata_companies.csv").exists():
        raw = wd.fetch(CACHE, refresh=refresh)
        comps = wd.parse_companies(raw["attributes"])
        facts = [
            *wd.parse_facts("employees", raw["facts_employees"]),
            *wd.parse_facts("revenue_usd", raw["facts_revenue_usd"]),
        ]
        wd.write_csv(comps, facts, DATA)
        source.write_text(
            json.dumps(
                {
                    "source": "Wikidata SPARQL endpoint",
                    "endpoint": wd.ENDPOINT,
                    "license": wd.LICENSE,
                    "retrieved": raw["meta"]["retrieved"],
                    "company_classes": list(wd.COMPANY_CLASSES),
                    "facts_queries": {p: wd.facts_query(p) for p in wd.FACT_PROPS},
                    "companies": len(comps),
                    "facts": len(facts),
                },
                indent=2,
            )
            + "\n"
        )
    comps, facts = wd.read_csv(DATA)
    return comps, facts, json.loads(source.read_text())


def checkpoints(retrieved: date) -> list[date]:
    years = [date(y, 12, 31) for y in range(INITIAL.year + 1, retrieved.year)]
    return [INITIAL, *years, retrieved]


class Warehouse:
    """Warehouse whose `companies` table is moved from one dated snapshot to the next."""

    def __init__(self, path: Path | str) -> None:
        url = path if isinstance(path, str) else f"sqlite:///{path}"
        self.engine: Engine = create_engine(url)
        metadata.drop_all(self.engine)
        metadata.create_all(self.engine)
        self.rows: dict[str, dict[str, Any]] = {}

    def move_to(self, snap: dict[str, dict[str, Any]]) -> wd.Delta:
        delta = wd.diff(self.rows, snap)
        ts = utcnow()
        with self.engine.begin() as c:
            if delta.inserts:
                c.execute(insert(companies), [{**r, "updated_at": ts} for r in delta.inserts])
            for r in delta.updates:
                c.execute(
                    update(companies)
                    .where(companies.c.qid == r["qid"])
                    .values(**{k: v for k, v in r.items() if k != "qid"}, updated_at=ts)
                )
            if delta.deletes:
                c.execute(delete(companies).where(companies.c.qid.in_(delta.deletes)))
        self.rows = dict(snap)
        return delta


def mapping(**settings: Any) -> CompiledMapping:
    data = load_mapping(MAPPING).config.model_dump()
    data["settings"].update(settings)
    return compile_mapping(data)


# -- checks -----------------------------------------------------------------------------------
def verify(hub: FakeHub, eng: SyncEngine, wh: Engine) -> dict[str, Any]:
    """Independent convergence check over every warehouse row and every live CRM record."""
    obj = eng.mapping.objects["companies"]
    owned = [
        f.name for f in obj.fields if (f.spec.policy, f.spec.direction) == ("source_wins", "to_crm")
    ]
    crm: dict[str, Any] = {}
    dupes = 0
    for rec in hub.live("companies"):
        k = obj.key_from_crm(rec.props) or ""
        dupes += k in crm
        crm[k] = rec
    mismatched_rows = mismatched_fields = missing = 0
    keys: set[str] = set()
    with wh.connect() as c:
        for row in c.execute(select(companies)).mappings():
            key = obj.key_from_row(row) or ""
            keys.add(key)
            rec = crm.get(key)
            if rec is None:
                missing += 1
                continue
            want, have = obj.warehouse_values(row), obj.crm_values(rec.props)
            bad = sum(1 for f in owned if have[f] != want[f])
            mismatched_fields += bad
            mismatched_rows += bad > 0
    return {
        "warehouse_rows": len(keys),
        "crm_live_records": len(crm),
        "fields_checked_per_row": len(owned),
        "missing_in_crm": missing,
        "mismatched_rows": mismatched_rows,
        "mismatched_fields": mismatched_fields,
        "crm_records_without_warehouse_row": sum(1 for k in crm if k not in keys),
        "duplicate_crm_records": dupes,
    }


def run(eng: SyncEngine) -> dict[str, Any]:
    m = eng.crm.metrics
    t0, s0 = m.transport_errors, m.server_errors
    r = run_sync(eng)
    r["transport_errors"] = m.transport_errors - t0  # client timeouts
    r["server_errors_seen"] = m.server_errors - s0
    r["writes"] = writes(r)
    return r


def writes(r: dict[str, Any]) -> int:
    rep = r["report"]
    return int(
        rep["created"] + rep["updated"] + rep["archived"] + rep["written_back"] + rep["forgotten"]
    )


# -- scenarios --------------------------------------------------------------------------------
def replay(
    tmp: Path,
    name: str,
    snaps: list[tuple[date, dict[str, dict[str, Any]]]],
    faults: FaultConfig,
    log: Any,
    **settings: Any,
) -> dict[str, Any]:
    hub = FakeHub(SystemClock(), faults)
    srv = Server(hub)
    wh = Warehouse(tmp / f"{name}_wh.db")
    eng = SyncEngine(
        mapping(**settings),
        warehouse=wh.engine,
        state=create_engine(f"sqlite:///{tmp / (name + '_state.db')}"),
        http=httpx.Client(base_url=srv.url, timeout=CLIENT_TIMEOUT_S),
        token="bench-token",
        seed=7,
    )
    runs: list[dict[str, Any]] = []
    t_all = time.perf_counter()
    for i, (at, snap) in enumerate(snaps):
        delta = wh.move_to(snap)
        before = dict(hub.stats)
        if i:
            time.sleep(2.5)  # let the fake's search index catch up (search_lag_seconds = 2)
        r = run(eng)
        r["as_of"] = at.isoformat()
        r["kind"] = "initial" if i == 0 else "incremental"
        r["warehouse_delta"] = delta.counts()
        r["records_in_warehouse"] = len(snap)
        r["injected"] = {
            k: v - before.get(k, 0) for k, v in hub.stats.items() if v != before.get(k, 0)
        }
        r["records_per_second"] = round(
            (len(snap) if i == 0 else delta_size(delta)) / max(r["wall_seconds"], 1e-9), 1
        )
        runs.append(r)
        log(
            f"{name} {r['kind']:<11} {at} rows={len(snap):>6} delta={delta_size(delta):>5} "
            f"calls={r['api_calls']:>4} writes={r['writes']:>5} 429={r['rate_limited']:>3} "
            f"retries={r['retries']:>3} dlq={r['report']['dead_lettered']:>3} "
            f"{r['wall_seconds']}s"
        )
    replay_wall = time.perf_counter() - t_all

    # Converge: faults off, re-run until no dead letters are left (bounded).
    hub.configure(**NO_FAULTS)
    converge: list[dict[str, Any]] = []
    for _ in range(4):
        time.sleep(2.5)
        r = run(eng)
        converge.append(r)
        open_dlq = len(eng.dead_letters())
        log(f"{name} converge    calls={r['api_calls']} writes={r['writes']} dlq_open={open_dlq}")
        if not eng.dead_letters() and r["writes"] == 0:
            break
    check = verify(hub, eng, wh.engine)
    log(f"{name} verify      {check}")

    # Idempotence: two more runs. The first pages through our own writes echoing back via
    # search; neither may write anything.
    idem: list[dict[str, Any]] = []
    for _ in range(2):
        time.sleep(2.5)
        r = run(eng)
        idem.append(r)
        log(f"{name} idempotence calls={r['api_calls']} writes={r['writes']} plan={r['plan']}")
    srv.stop()

    total = lambda k: sum(int(r[k]) for r in runs)  # noqa: E731
    return {
        "runs": runs,
        "converge_runs": converge,
        "verify_after_convergence": check,
        "idempotence_runs": idem,
        "totals_replay": {
            "runs": len(runs),
            "api_calls": total("api_calls"),
            "writes": total("writes"),
            "rate_limited_429s_seen_by_client": total("rate_limited"),
            "retries": total("retries"),
            "bisections": total("bisections"),
            "dead_lettered": sum(int(r["report"]["dead_lettered"]) for r in runs),
            "client_timeouts": total("transport_errors"),
            "server_errors_seen_by_client": total("server_errors_seen"),
            "wall_seconds": round(replay_wall, 1),
        },
        "totals_including_convergence_and_idempotence": {
            "api_calls": total("api_calls") + sum(int(r["api_calls"]) for r in converge + idem),
        },
        "server_stats": dict(hub.stats),
        "limiter": eng.limiter.stats.__dict__,
    }


def delta_size(d: wd.Delta) -> int:
    return len(d.inserts) + len(d.updates) + len(d.deletes)


def naive(
    tmp: Path, snaps: list[tuple[date, dict[str, dict[str, Any]]]], log: Any
) -> dict[str, Any]:
    """One request per record, no batching, no limits, no faults; handed the exact delta."""
    hub = FakeHub(SystemClock(), FaultConfig(search_per_second=None))
    srv = Server(hub)
    wh = Warehouse(tmp / "naive_wh.db")
    obj = mapping().objects["companies"]
    http = httpx.Client(base_url=srv.url, timeout=120.0, headers={"Authorization": "Bearer naive"})
    path = f"/crm/v3/objects/{obj.crm_object}"
    idp = {"idProperty": obj.spec.match.crm}
    runs = []
    t_all = time.perf_counter()
    for at, snap in snaps:
        delta = wh.move_to(snap)
        calls, t0 = 0, time.perf_counter()
        for row in [*delta.inserts, *delta.updates]:
            key = obj.key_from_row(row) or ""
            props = {k: ("" if v is None else v) for k, v in obj.warehouse_values(row).items()}
            r = http.get(f"{path}/{key}", params=idp)
            calls += 1
            if r.status_code == 404:
                http.post(path, json={"properties": props})
            else:
                http.patch(f"{path}/{r.json()['id']}", json={"properties": props})
            calls += 1
        for key in delta.deletes:
            r = http.get(f"{path}/{key}", params=idp)
            calls += 1
            if r.status_code == 200:
                http.delete(f"{path}/{r.json()['id']}")
                calls += 1
        runs.append(
            {
                "as_of": at.isoformat(),
                "api_calls": calls,
                "wall_seconds": round(time.perf_counter() - t0, 2),
            }
        )
        log(f"naive {at} delta={delta_size(delta):>5} calls={calls:>5}")
    srv.stop()
    total_calls = sum(r["api_calls"] for r in runs)
    return {
        "runs": runs,
        "totals_replay": {
            "runs": len(runs),
            "api_calls": total_calls,
            "wall_seconds": round(time.perf_counter() - t_all, 1),
            "projected_hours_at_100_per_10s": round(total_calls / 10 / 3600, 2),
        },
    }


# -- report -----------------------------------------------------------------------------------
def render_md(d: dict[str, Any]) -> str:
    src, e, n = d["data"], d["engine"], d["naive"]
    et, nt = e["totals_replay"], n["totals_replay"]
    v = e["verify_after_convergence"]
    out = [
        f"# Benchmark on real company data ({src['companies']:,} Wikidata companies)",
        "",
        f"Generated by `bench/run_real_bench.py` on {d['generated']}. Machine: {d['machine']}.",
        f"Data: {src['source']} ({src['endpoint']}), licence {src['license']}, retrieved "
        f"{src['retrieved']}: {src['companies']:,} companies, {src['facts']:,} dated "
        "employee/revenue statements.",
        f"Note: {d['note']}",
        "",
        "## Headline",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Sync runs replayed (1 initial + incrementals) | {et['runs']} |",
        f"| Records in the initial load / at the end | {e['runs'][0]['records_in_warehouse']:,} / "
        f"{e['runs'][-1]['records_in_warehouse']:,} |",
        f"| Real warehouse changes replayed (inserts + updates + deletes after the initial load) | "
        f"{d['changes_replayed']:,} |",
        f"| API calls, engine (replay, limits + faults on) | {et['api_calls']:,} |",
        f"| API calls, naive one-request-per-record | {nt['api_calls']:,} |",
        f"| Ratio | {d['ratio_naive_over_engine']}x fewer |",
        f"| Engine initial load, 100 req/10 s enforced | {e['runs'][0]['wall_seconds']} s "
        f"({e['runs'][0]['records_per_second']} records/s) |",
        f"| Engine wall time, whole replay incl. 2.5 s index-lag pauses | {et['wall_seconds']} s |",
        f"| Naive projected time at 100 req/10 s | {nt['projected_hours_at_100_per_10s']} h |",
        f"| Faults injected by the fake | {d['faults_injected']} |",
        "| 429s seen by the client / retries / bisections | "
        f"{et['rate_limited_429s_seen_by_client']} / {et['retries']} / {et['bisections']} |",
        f"| Records dead-lettered during the replay (then retried) | {et['dead_lettered']} |",
        f"| Mismatches after convergence ({v['warehouse_rows']:,} rows x "
        f"{v['fields_checked_per_row']} fields) | {v['mismatched_fields']} fields in "
        f"{v['mismatched_rows']} rows; {v['missing_in_crm']} missing; "
        f"{v['duplicate_crm_records']} duplicates; "
        f"{v['crm_records_without_warehouse_row']} orphans |",
        f"| Idempotence: writes on the 2 re-runs | {[r['writes'] for r in e['idempotence_runs']]} "
        f"(API calls {[r['api_calls'] for r in e['idempotence_runs']]}) |",
        "",
        "## Per run (engine, Free/Starter limits enforced + faults)",
        "",
        "| As of | Rows | +ins | ~upd | -del | API calls | Writes | 429s | Retries | DLQ "
        "| Wall s |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in e["runs"]:
        wd_ = r["warehouse_delta"]
        out.append(
            f"| {r['as_of']} | {r['records_in_warehouse']:,} | {wd_['inserts']} | "
            f"{wd_['updates']} | {wd_['deletes']} | {r['api_calls']} | {r['writes']} | "
            f"{r['rate_limited']} | {r['retries']} | {r['report']['dead_lettered']} | "
            f"{r['wall_seconds']} |"
        )
    u = d.get("unthrottled")
    if u:
        ut = u["totals_replay"]
        out += [
            "",
            "## Unthrottled, no faults (raw engine throughput on localhost)",
            "",
            f"Initial load: {u['runs'][0]['records_in_warehouse']:,} records in "
            f"{u['runs'][0]['wall_seconds']} s ({u['runs'][0]['records_per_second']} records/s), "
            f"{u['runs'][0]['api_calls']} API calls. Whole replay: {ut['api_calls']:,} API calls, "
            f"{ut['wall_seconds']} s. Mismatches after convergence: "
            f"{u['verify_after_convergence']['mismatched_fields']}.",
        ]
    out += [
        "",
        "## Naive baseline per run",
        "",
        "| As of | API calls | Wall s (unthrottled) |",
        "|---|---:|---:|",
        *[f"| {r['as_of']} | {r['api_calls']:,} | {r['wall_seconds']} |" for r in n["runs"]],
        "",
    ]
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="re-download from Wikidata")
    ap.add_argument("--skip-unthrottled", action="store_true")
    ap.add_argument("--out", default=str(RESULTS / "real_bench.json"))
    ap.add_argument(
        "--load-warehouse",
        metavar="URL",
        help="only (re)create the companies table at this SQLAlchemy URL and exit "
        "(for a live HubSpot run with crm-sync plan/apply)",
    )
    ap.add_argument("--as-of", type=date.fromisoformat, help="snapshot date for --load-warehouse")
    ap.add_argument("--render", action="store_true", help="only re-render the .md from --out")
    a = ap.parse_args()
    if a.render:
        Path(a.out).with_suffix(".md").write_text(render_md(json.loads(Path(a.out).read_text())))
        return

    comps, facts, src = load_data(a.refresh)
    if a.load_warehouse:
        at = a.as_of or date.fromisoformat(src["retrieved"])
        n = len(Warehouse(a.load_warehouse).move_to(wd.snapshot(comps, facts, at)).inserts)
        print(f"loaded {n} companies as of {at} into {a.load_warehouse}")
        return
    snaps = [
        (at, wd.snapshot(comps, facts, at))
        for at in checkpoints(date.fromisoformat(src["retrieved"]))
    ]
    tmp = Path(tempfile.mkdtemp(prefix="crm-sync-real-bench-"))

    def log(msg: str) -> None:
        print(msg, flush=True)

    limits = {
        "requests_per_10s": HUBSPOT_PER_10S,
        "search_requests_per_second": 4,
        "daily_request_limit": 250_000,
        "search_lag_seconds": 2,
    }
    engine = replay(
        tmp,
        "engine",
        snaps,
        FaultConfig(
            requests_per_10s=HUBSPOT_PER_10S,
            search_per_second=HUBSPOT_SEARCH_PER_S,
            daily_limit=250_000,
            search_delay_ms=1000,
            retry_after_s=0.2,
            seed=42,
            **FAULTS,
        ),
        log,
        **limits,
    )
    unthrottled = None
    if not a.skip_unthrottled:
        unthrottled = replay(
            tmp,
            "unthrottled",
            snaps,
            FaultConfig(search_per_second=None, search_delay_ms=1000),
            log,
            requests_per_10s=1_000_000,
            search_requests_per_second=1_000_000,
            daily_request_limit=10_000_000,
            search_lag_seconds=2,
        )
    nv = naive(tmp, snaps, log)

    stats = engine["server_stats"]
    result: dict[str, Any] = {
        "generated": date.today().isoformat(),
        "machine": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "cpu": platform.processor() or platform.machine(),
        },
        "note": (
            "CRM side is the repo's fake HubSpot on localhost (same process), enforcing "
            "HubSpot's documented Free/Starter limits; API-call counts transfer to HubSpot, "
            "wall-clock times do not."
        ),
        "data": src,
        "checkpoints": [at.isoformat() for at, _ in snaps],
        "changes_replayed": sum(
            r["warehouse_delta"]["inserts"]
            + r["warehouse_delta"]["updates"]
            + r["warehouse_delta"]["deletes"]
            for r in engine["runs"][1:]
        ),
        "faults_config": FAULTS,
        "faults_injected": {
            k: stats.get(k, 0)
            for k in (
                "429_injected",
                "429_rate_limit",
                "5xx",
                "applied_then_5xx",
                "timeouts",
                "partial_batch",
            )
        },
        "ratio_naive_over_engine": round(
            nv["totals_replay"]["api_calls"] / engine["totals_replay"]["api_calls"], 1
        ),
        "engine": engine,
        "unthrottled": unthrottled,
        "naive": nv,
    }
    Path(a.out).write_text(json.dumps(result, indent=2) + "\n")
    md = Path(a.out).with_suffix(".md")
    md.write_text(render_md(result))
    print(f"\nwrote {a.out} and {md}")


if __name__ == "__main__":
    main()

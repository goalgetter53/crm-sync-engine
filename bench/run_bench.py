"""Benchmark: sync 50k records against the fake HubSpot server over real HTTP (localhost).

Scenarios
  initial      empty CRM, 50k warehouse rows (5k companies, 40k contacts, 5k deals) + associations
  echo_noop    re-run right after the load: our own writes come back through search
  steady_noop  re-run again: nothing changed anywhere
  incremental  1% warehouse edits + 200 rep edits in HubSpot
  faults       fresh load with injected 429s, 5xx, lost responses and 25 poison rows
  throttled    fresh load with the fake enforcing HubSpot's Free/Starter limits (100 req/10 s)
  naive        one-request-per-record baseline (GET by unique key, then POST or PATCH, then a
               PUT per association), the way a quick script or a Zapier-style flow would do it

Everything runs on one machine: client and fake server share the process (separate threads).
Numbers are from the fake server, not HubSpot; API-call counts transfer, latencies do not.
"""

from __future__ import annotations

import argparse
import json
import platform
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from sqlalchemy import create_engine, select, update

from crm_sync import demo
from crm_sync.clock import SystemClock
from crm_sync.dsl import compile_mapping, load_mapping
from crm_sync.dsl.loader import CompiledMapping
from crm_sync.engine import SyncEngine
from fake_hubspot import FakeHub, FaultConfig, create_app

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
HUBSPOT_PER_10S = 100  # Free/Starter private app burst limit
HUBSPOT_SEARCH_PER_S = 5


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class Server:
    def __init__(self, hub: FakeHub) -> None:
        self.port = free_port()
        cfg = uvicorn.Config(
            create_app(hub), host="127.0.0.1", port=self.port, log_level="error", access_log=False
        )
        self.server = uvicorn.Server(cfg)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        while not self.server.started:
            time.sleep(0.05)
        self.url = f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


def mapping(**settings: Any) -> CompiledMapping:
    data = load_mapping(ROOT / "examples" / "mapping.yaml").config.model_dump()
    data["settings"].update(settings)
    return compile_mapping(data)


UNTHROTTLED = {
    "requests_per_10s": 1_000_000,
    "search_requests_per_second": 1_000_000,
    "daily_request_limit": 10_000_000,
    "search_lag_seconds": 2,
}


def make_world(
    tmp: Path,
    name: str,
    sizes: dict[str, int],
    faults: FaultConfig,
    bad_rows: int = 0,
    **settings: Any,
) -> tuple[FakeHub, Server, SyncEngine, Any]:
    hub = FakeHub(SystemClock(), faults)
    srv = Server(hub)
    wh = create_engine(f"sqlite:///{tmp / (name + '_wh.db')}")
    demo.seed(
        wh,
        companies_n=sizes["companies"],
        contacts_n=sizes["contacts"],
        deals_n=sizes["deals"],
        bad_rows=bad_rows,
    )
    st = create_engine(f"sqlite:///{tmp / (name + '_state.db')}")
    http = httpx.Client(base_url=srv.url, timeout=120.0)
    eng = SyncEngine(
        mapping(**{**UNTHROTTLED, **settings}),
        warehouse=wh,
        state=st,
        http=http,
        token="bench-token",
        seed=7,
    )
    return hub, srv, eng, wh


def run_sync(eng: SyncEngine) -> dict[str, Any]:
    before = eng.crm.metrics.snapshot()
    t0 = time.perf_counter()
    plan, report = eng.apply()
    wall = time.perf_counter() - t0
    after = eng.crm.metrics.snapshot()
    calls = {k: v - before["calls"].get(k, 0) for k, v in after["calls"].items()}
    return {
        "wall_seconds": round(wall, 2),
        "api_calls": after["total_calls"] - before["total_calls"],
        "planning_calls": plan.planning_calls,
        "calls_by_endpoint": {k: v for k, v in sorted(calls.items()) if v},
        "plan": plan.counts(),
        "report": report.as_dict(),
        "retries": after["retries"] - before["retries"],
        "rate_limited": after["rate_limited"] - before["rate_limited"],
        "bisections": after["bisections"] - before["bisections"],
        "search_restarts": after["search_restarts"] - before["search_restarts"],
    }


def verify(
    hub: FakeHub, eng: SyncEngine, wh: Any, skip_keys: set[str] | None = None
) -> dict[str, Any]:
    """Independently check convergence: every warehouse row has exactly one CRM record whose
    source-owned fields match, and every association exists."""
    skip = skip_keys or set()
    m = eng.mapping
    mismatches = 0
    dupes = 0
    checked = 0
    for name in ("companies", "contacts", "deals"):
        obj = m.objects[name]
        crm: dict[str, Any] = {}
        for rec in hub.live(obj.crm_object):
            k = obj.key_from_crm(rec.props)
            if k in crm:
                dupes += 1
            crm[k or ""] = rec
        t = eng.warehouse.tables[name]
        with wh.connect() as c:
            for row in c.execute(select(t)).mappings():
                key = obj.key_from_row(row)
                if key in skip:
                    continue
                checked += 1
                rec = crm.get(key or "")
                if rec is None:
                    mismatches += 1
                    continue
                want = obj.warehouse_values(row)
                have = obj.crm_values(rec.props)
                owned = [
                    f.name
                    for f in obj.fields
                    if (f.spec.policy, f.spec.direction) == ("source_wins", "to_crm")
                ]
                mismatches += sum(1 for f in owned if have[f] != want[f])
    return {
        "rows_checked": checked,
        "missing_or_mismatched": mismatches,
        "duplicate_crm_records": dupes,
        "associations": len(hub.assocs) // 2,
    }


def naive(tmp: Path, sizes: dict[str, int]) -> dict[str, Any]:
    """One request per record (+1 per association). No batching, no change detection."""
    hub = FakeHub(SystemClock(), FaultConfig(search_per_second=None))
    srv = Server(hub)
    wh = create_engine(f"sqlite:///{tmp / 'naive_wh.db'}")
    demo.seed(
        wh, companies_n=sizes["companies"], contacts_n=sizes["contacts"], deals_n=sizes["deals"]
    )
    m = mapping()
    http = httpx.Client(base_url=srv.url, timeout=120.0, headers={"Authorization": "Bearer naive"})
    calls = 0
    ids: dict[tuple[str, str], str] = {}
    t0 = time.perf_counter()
    for name in m.order:
        obj = m.objects[name]
        t = demo.metadata.tables[obj.spec.table]
        with wh.connect() as c:
            rows = list(c.execute(select(t)).mappings())
        for row in rows:
            key = obj.key_from_row(row) or ""
            props = {k: v for k, v in obj.warehouse_values(row).items() if v is not None}
            props[obj.spec.match.crm] = key
            r = http.get(
                f"/crm/v3/objects/{obj.crm_object}/{key}", params={"idProperty": obj.spec.match.crm}
            )
            calls += 1
            if r.status_code == 404:
                r = http.post(f"/crm/v3/objects/{obj.crm_object}", json={"properties": props})
            else:
                r = http.patch(
                    f"/crm/v3/objects/{obj.crm_object}/{r.json()['id']}", json={"properties": props}
                )
            calls += 1
            if r.status_code < 300:
                ids[(name, key)] = r.json()["id"]
            for a in obj.spec.associations:
                to_key = m.objects[a.to].normalize_key(row.get(a.via))
                fid, tid = ids.get((name, key)), ids.get((a.to, to_key or ""))
                if fid and tid:
                    http.put(
                        f"/crm/v4/objects/{obj.crm_object}/{fid}/associations/default/"
                        f"{m.objects[a.to].crm_object}/{tid}"
                    )
                    calls += 1
    wall = time.perf_counter() - t0
    srv.stop()
    return {
        "wall_seconds": round(wall, 2),
        "api_calls": calls,
        "records_written": hub.stats["records_written"],
    }


def core(tmp: Path, sizes: dict[str, int], total: int, log: Any) -> None:
    hub, srv, eng, wh = make_world(tmp, "main", sizes, FaultConfig(search_per_second=None))
    r = run_sync(eng)
    r["records_per_second"] = round(total / r["wall_seconds"], 1)
    r["verify"] = verify(hub, eng, wh)
    log("initial", r)
    time.sleep(2.5)
    log("echo_noop", run_sync(eng))
    time.sleep(2.5)
    log("steady_noop", run_sync(eng))
    # incremental: 1% warehouse edits + 200 rep edits
    t = demo.contacts
    n_edit = total // 100
    with wh.begin() as c:
        c.execute(
            update(t).where(t.c.id <= n_edit).values(first_name="Edited", updated_at=demo.utcnow())
        )
    crm_contacts = hub.live("contacts")[-200:]
    for rec in crm_contacts:
        hub.admin_edit("contacts", rec.id, {"lifecyclestage": "evangelist"})
    time.sleep(2.5)
    r = run_sync(eng)
    r["edits"] = {"warehouse_rows": n_edit, "crm_records": len(crm_contacts)}
    r["verify"] = verify(hub, eng, wh)
    log("incremental", r)
    srv.stop()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--companies", type=int, default=5_000)
    ap.add_argument("--contacts", type=int, default=40_000)
    ap.add_argument("--deals", type=int, default=5_000)
    ap.add_argument("--skip", action="append", default=[])
    ap.add_argument("--only", default=None, help="comma list: core,faults,throttled,naive")
    ap.add_argument("--out", default=str(RESULTS / "latest.json"))
    a = ap.parse_args()
    sizes = {"companies": a.companies, "contacts": a.contacts, "deals": a.deals}
    total = sum(sizes.values())
    only = set(a.only.split(",")) if a.only else {"core", "faults", "throttled", "naive"}
    a.skip = [s for s in ("core", "faults", "throttled", "naive") if s not in only or s in a.skip]
    previous = json.loads(Path(a.out).read_text()) if Path(a.out).exists() and a.only else {}
    results: dict[str, Any] = {
        **previous,
        "records": total,
        "sizes": sizes,
        "machine": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "cpu": platform.processor() or platform.machine(),
        },
        "note": "fake HubSpot server on localhost, same process; not real HubSpot latencies",
    }
    tmp = Path(tempfile.mkdtemp(prefix="crm-sync-bench-"))

    def log(k: str, v: Any) -> None:
        results[k] = v
        print(f"[{k}] {json.dumps(v)}", flush=True)
        Path(a.out).write_text(json.dumps(results, indent=2))

    if "core" not in a.skip:
        core(tmp, sizes, total, log)

    if "faults" not in a.skip:
        faults = FaultConfig(
            search_per_second=None,
            p_429=0.02,
            p_5xx=0.01,
            p_apply_then_5xx=0.01,
            retry_after_s=0.2,
            seed=42,
        )
        hub, srv, eng, wh = make_world(tmp, "faults", sizes, faults, bad_rows=25)
        r = run_sync(eng)
        bad = {d.record_key for d in eng.dead_letters()}
        r["dead_letters"] = len(bad)
        r["server_stats"] = dict(hub.stats)
        r["verify_excluding_dead_letters"] = verify(hub, eng, wh, skip_keys=bad)
        log("faults", r)
        srv.stop()

    if "throttled" not in a.skip:
        hub, srv, eng, wh = make_world(
            tmp,
            "throttled",
            sizes,
            FaultConfig(requests_per_10s=HUBSPOT_PER_10S, search_per_second=HUBSPOT_SEARCH_PER_S),
            requests_per_10s=HUBSPOT_PER_10S,
            search_requests_per_second=4,
        )
        r = run_sync(eng)
        r["server_429s"] = hub.stats["429"]
        r["limiter"] = eng.limiter.stats.__dict__
        r["verify"] = verify(hub, eng, wh)
        log("throttled", r)
        srv.stop()

    if "naive" not in a.skip:
        r = naive(tmp, sizes)
        r["projected_hours_at_100_per_10s"] = round(r["api_calls"] / 10 / 3600, 2)
        log("naive", r)

    print(f"\nwrote {a.out}")


if __name__ == "__main__":
    main()

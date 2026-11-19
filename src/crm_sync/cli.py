"""``crm-sync`` command line: validate | plan | apply | dlq | replay | demo-seed."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path

import httpx
from sqlalchemy import create_engine

from crm_sync import demo
from crm_sync.dsl.loader import MappingError, load_mapping
from crm_sync.engine.render import plan_to_json, render_plan
from crm_sync.engine.sync import SyncEngine

DEFAULT_WAREHOUSE = "sqlite:///warehouse.db"
DEFAULT_STATE = "sqlite:///.crm-sync-state.db"
DEFAULT_HUBSPOT = "https://api.hubapi.com"


def _engine(args: argparse.Namespace, http: httpx.Client | None) -> SyncEngine:
    mapping = load_mapping(Path(args.mapping))
    client = http or httpx.Client(
        base_url=os.environ.get("HUBSPOT_BASE_URL", DEFAULT_HUBSPOT), timeout=30.0
    )
    return SyncEngine(
        mapping,
        warehouse=create_engine(os.environ.get("WAREHOUSE_URL", DEFAULT_WAREHOUSE)),
        state=create_engine(os.environ.get("STATE_URL", DEFAULT_STATE)),
        http=client,
        token=os.environ.get("HUBSPOT_ACCESS_TOKEN"),
    )


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="crm-sync", description="Bi-directional warehouse <-> HubSpot sync engine"
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate", help="validate a mapping file")
    v.add_argument("mapping")

    for name, help_ in (("plan", "show what would change"), ("apply", "plan, then execute")):
        sp = sub.add_parser(name, help=help_)
        sp.add_argument("mapping")
        sp.add_argument("--full-refresh", action="store_true", help="ignore watermarks")
        sp.add_argument("--json", action="store_true", help="machine-readable output")
        sp.add_argument("--max-records", type=int, default=25)
        if name == "plan":
            sp.add_argument(
                "--detailed-exitcode", action="store_true", help="exit 2 when there are changes"
            )

    d = sub.add_parser("dlq", help="list open dead letters")
    d.add_argument("mapping")

    r = sub.add_parser("replay", help="re-drive dead letters with fresh data")
    r.add_argument("mapping")
    r.add_argument("--id", type=int, action="append", dest="ids")

    s = sub.add_parser("demo-seed", help="create a synthetic warehouse")
    s.add_argument("--companies", type=int, default=50)
    s.add_argument("--contacts", type=int, default=500)
    s.add_argument("--deals", type=int, default=100)
    s.add_argument("--bad-rows", type=int, default=2)
    return p


def main(argv: Sequence[str] | None = None, *, http: httpx.Client | None = None) -> int:
    args = _parser().parse_args(argv)
    out = sys.stdout
    try:
        if args.cmd == "validate":
            m = load_mapping(Path(args.mapping))
            for w in m.warnings:
                print(f"warning: {w}", file=out)
            print(f"OK: {len(m.objects)} object(s), sync order: {' -> '.join(m.order)}", file=out)
            return 0

        if args.cmd == "demo-seed":
            url = os.environ.get("WAREHOUSE_URL", DEFAULT_WAREHOUSE)
            counts = demo.seed(
                create_engine(url),
                companies_n=args.companies,
                contacts_n=args.contacts,
                deals_n=args.deals,
                bad_rows=args.bad_rows,
            )
            print(f"seeded {url}: {counts}", file=out)
            return 0

        eng = _engine(args, http)
        if args.cmd == "plan":
            plan = eng.plan(full_refresh=args.full_refresh)
            if args.json:
                print(json.dumps(plan_to_json(plan), indent=2, default=str), file=out)
            else:
                print(render_plan(plan, color=out.isatty(), max_records=args.max_records), file=out)
            return 2 if (args.detailed_exitcode and plan.has_changes) else 0

        if args.cmd == "apply":
            plan, report = eng.apply(full_refresh=args.full_refresh)
            if args.json:
                print(json.dumps({"plan": plan.counts(), "report": report.as_dict()}), file=out)
            else:
                print(render_plan(plan, color=out.isatty(), max_records=args.max_records), file=out)
                print("", file=out)
                r = report.as_dict()
                print(
                    "Apply complete: "
                    + ", ".join(f"{k}={v}" for k, v in r.items() if v and k != "failed"),
                    file=out,
                )
                if report.dead_lettered:
                    print(
                        f"{report.dead_lettered} operation(s) dead-lettered; "
                        "see `crm-sync dlq`, fix, then `crm-sync replay`.",
                        file=out,
                    )
            return 0

        if args.cmd == "dlq":
            entries = eng.dead_letters()
            for e in entries:
                print(
                    f"#{e.id:<5} {e.object}:{e.record_key}  op={e.op} attempts={e.attempts}\n"
                    f"       {e.error}",
                    file=out,
                )
            print(f"{len(entries)} open dead letter(s)", file=out)
            return 0

        if args.cmd == "replay":
            res = eng.replay(args.ids)
            print(
                f"replayed {res.attempted}: {res.resolved} resolved, "
                f"{res.still_failing} still failing",
                file=out,
            )
            return 0 if res.still_failing == 0 else 1
    except MappingError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

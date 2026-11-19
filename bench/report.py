"""Render bench/results/latest.json as a Markdown table (bench/results/RESULTS.md)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"


def main() -> None:
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else RESULTS / "latest.json"
    d = json.loads(src.read_text())
    rows = [
        ("initial", "Initial load, empty CRM"),
        ("echo_noop", "Re-run right after the load (own writes echo back via search)"),
        ("steady_noop", "Re-run, nothing changed"),
        ("incremental", "1% warehouse edits + 200 rep edits in HubSpot"),
        ("faults", "Initial load with 429s/5xx/lost responses + 25 poison rows"),
        ("throttled", "Initial load, fake enforces 100 req/10 s + 5 search/s"),
        ("naive", "Baseline: one request per record and per association"),
    ]
    out = [
        f"# Benchmark results ({d['records']:,} records)",
        "",
        f"Sizes: {d['sizes']}. Machine: {d['machine']}.",
        f"Note: {d['note']}.",
        "",
        "| Scenario | What | API calls | Wall time (s) | Notes |",
        "|---|---|---:|---:|---|",
    ]
    for key, what in rows:
        r = d.get(key)
        if not r:
            continue
        notes = []
        if "verify" in r:
            v = r["verify"]
            notes.append(
                f"verified {v['rows_checked']:,} rows: {v['missing_or_mismatched']} mismatches, "
                f"{v['duplicate_crm_records']} duplicates"
            )
        if "verify_excluding_dead_letters" in r:
            v = r["verify_excluding_dead_letters"]
            notes.append(
                f"{r['dead_letters']} dead letters; {v['rows_checked']:,} other rows: "
                f"{v['missing_or_mismatched']} mismatches, "
                f"{v['duplicate_crm_records']} duplicates; "
                f"injected {r['server_stats']}; {r['retries']} retries, "
                f"{r['bisections']} bisections"
            )
        if "server_429s" in r:
            notes.append(f"server 429s: {r['server_429s']}")
        if "projected_hours_at_100_per_10s" in r:
            notes.append(f"projected {r['projected_hours_at_100_per_10s']} h at 100 req/10 s")
        out.append(
            f"| `{key}` | {what} | {r['api_calls']:,} | {r['wall_seconds']} | {'; '.join(notes)} |"
        )
    (RESULTS / "RESULTS.md").write_text("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()

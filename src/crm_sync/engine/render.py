"""Terraform-style plan rendering: ``+`` create, ``~`` update, ``-`` archive, ``!`` conflict,
``<`` write-back to the warehouse."""

from __future__ import annotations

import json
from typing import Any

from crm_sync.engine.plan import Plan, RecordPlan

_COLORS = {"+": "\033[32m", "~": "\033[33m", "-": "\033[31m", "!": "\033[35m", "<": "\033[36m"}
_RESET = "\033[0m"


def _q(v: str | None) -> str:
    return "(empty)" if v is None else json.dumps(v)


def _line(sym: str, text: str, color: bool) -> str:
    return f"{_COLORS[sym]}{text}{_RESET}" if color else text


def _why(reason: str) -> str:
    return f"   # {reason}" if ":" in reason else ""


def _record_lines(r: RecordPlan, color: bool) -> list[str]:
    out: list[str] = []
    ident = f"{r.key}" + (f"  (id {r.crm_id})" if r.crm_id else "")
    if r.action == "create":
        out.append(_line("+", f"  + create    {ident}", color))
        for d in r.decisions:
            if d.write_crm:
                out.append(_line("+", f"      + {d.crm_property:<22} = {_q(d.resolved)}", color))
    elif r.action in ("update", "writeback"):
        sym = "~" if r.action == "update" else "<"
        word = "update" if r.action == "update" else "writeback"
        out.append(_line(sym, f"  {sym} {word:<10}{ident}", color))
        for d in r.decisions:
            if d.write_crm:
                change = f"      ~ {d.crm_property:<22} {_q(d.crm)} -> {_q(d.resolved)}"
                out.append(_line("~", change + ("" if d.conflict else _why(d.reason)), color))
            if d.write_wh:
                out.append(
                    _line(
                        "<",
                        f"      < {d.crm_property:<22} warehouse {_q(d.wh)} <- {_q(d.resolved)}",
                        color,
                    )
                )
    elif r.action == "archive":
        out.append(_line("-", f"  - archive   {ident}  (row deleted in warehouse)", color))
    elif r.action == "refresh" and r.conflicts:
        out.append(
            _line("!", f"  ! keep      {ident}  (conflict resolved in favour of the CRM)", color)
        )
    elif r.action == "crm_deleted":
        out.append(_line("!", f"  ! deleted in CRM: {ident}  (respecting deletion)", color))
    for d in r.conflicts:
        out.append(
            _line(
                "!",
                f"      ! {d.crm_property:<22} conflict wh={_q(d.wh)} crm={_q(d.crm)} "
                f"-> {_q(d.resolved)} [{d.reason}]",
                color,
            )
        )
    return out


def render_plan(plan: Plan, *, color: bool = False, max_records: int = 25) -> str:
    lines: list[str] = []
    by_obj: dict[str, list[RecordPlan]] = {}
    for r in plan.records:
        if r.action == "forget" or (r.action == "refresh" and not r.conflicts):
            continue
        by_obj.setdefault(r.object, []).append(r)
    for obj, recs in by_obj.items():
        lines.append(f"# {obj}")
        for r in recs[:max_records]:
            lines.extend(_record_lines(r, color))
        if len(recs) > max_records:
            lines.append(f"  ... and {len(recs) - max_records} more {obj} change(s)")
        lines.append("")
    assoc: dict[tuple[str, str], list[int]] = {}
    for a in plan.assoc:
        c = assoc.setdefault((a.from_object, a.to_object), [0, 0])
        c[0 if a.op == "add" else 1] += 1
    for (f, t), (add, rem) in assoc.items():
        lines.append(f"# associations {f} -> {t}: +{add} -{rem}")
    for w in plan.warnings:
        lines.append(f"warning: {w}")
    lines.append(summary_line(plan))
    return "\n".join(lines)


def summary_line(plan: Plan) -> str:
    c = plan.counts()
    if not plan.has_changes:
        tail = f" ({c.get('refresh', 0)} state refreshes)" if c.get("refresh") else ""
        return f"No changes. CRM and warehouse are in sync.{tail}"
    return (
        f"Plan: {c.get('create', 0)} to create, {c.get('update', 0)} to update, "
        f"{c.get('archive', 0)} to archive, {c.get('writeback_fields', 0)} field write-back(s), "
        f"{c.get('conflicts', 0)} conflict(s) resolved, "
        f"{c.get('assoc_add', 0)} association(s) to add, {c.get('assoc_remove', 0)} to remove."
    )


def plan_to_json(plan: Plan) -> dict[str, Any]:
    return {
        "summary": plan.counts(),
        "records": [
            {
                "object": r.object,
                "key": r.key,
                "action": r.action,
                "crm_id": r.crm_id,
                "changes": [
                    {
                        "property": d.crm_property,
                        "warehouse": d.wh,
                        "crm": d.crm,
                        "resolved": d.resolved,
                        "write_crm": d.write_crm,
                        "write_warehouse": d.write_wh,
                        "conflict": d.conflict,
                        "reason": d.reason,
                    }
                    for d in r.decisions
                    if d.write_crm or d.write_wh or d.conflict
                ],
            }
            for r in plan.records
            if r.action != "refresh"
        ],
        "associations": [a.__dict__ for a in plan.assoc],
        "warnings": plan.warnings,
        "planning_api_calls": plan.planning_calls,
    }

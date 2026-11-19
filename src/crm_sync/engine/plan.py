"""Plan data model: what a sync *would* do, computed before anything is written."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from crm_sync.dsl.loader import CompiledObject
from crm_sync.engine.merge import FieldDecision, merge_field
from crm_sync.state.store import BaseRecord
from crm_sync.values import CanonValue, parse_hs_timestamp

Action = Literal["create", "update", "writeback", "refresh", "archive", "forget", "crm_deleted"]


@dataclass
class RecordPlan:
    object: str
    key: str
    action: Action
    wh_pk: Any = None
    crm_id: str | None = None
    row: dict[str, Any] | None = None
    wh_hash: str | None = None
    wh_ts: int | None = None
    crm_ts: int | None = None
    decisions: list[FieldDecision] = field(default_factory=list)
    base: BaseRecord | None = None

    @property
    def crm_writes(self) -> dict[str, CanonValue]:
        return {d.crm_property: d.resolved for d in self.decisions if d.write_crm}

    @property
    def wh_writes(self) -> list[FieldDecision]:
        return [d for d in self.decisions if d.write_wh]

    @property
    def conflicts(self) -> list[FieldDecision]:
        return [d for d in self.decisions if d.conflict]

    @property
    def op_id(self) -> str:
        """Deterministic idempotency key for this record operation."""
        material = json.dumps(
            [
                self.object,
                self.key,
                self.action,
                sorted(self.crm_writes.items()),
                sorted((self.base.fields if self.base else {}).items()),
            ],
            default=str,
        )
        return hashlib.sha256(material.encode()).hexdigest()[:32]


@dataclass(frozen=True)
class AssocChange:
    from_object: str
    to_object: str
    from_key: str
    to_key: str
    op: Literal["add", "remove"]


@dataclass
class Plan:
    records: list[RecordPlan] = field(default_factory=list)
    assoc: list[AssocChange] = field(default_factory=list)
    watermarks: dict[tuple[str, str], int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    planning_calls: int = 0

    def counts(self) -> dict[str, int]:
        c: Counter[str] = Counter(r.action for r in self.records)
        c["writeback_fields"] = sum(len(r.wh_writes) for r in self.records)
        c["conflicts"] = sum(len(r.conflicts) for r in self.records)
        c["assoc_add"] = sum(1 for a in self.assoc if a.op == "add")
        c["assoc_remove"] = sum(1 for a in self.assoc if a.op == "remove")
        return dict(c)

    @property
    def is_empty(self) -> bool:
        return not self.records and not self.assoc

    @property
    def has_changes(self) -> bool:
        """True if applying would touch the CRM or the warehouse (refreshes are state-only)."""
        return bool(self.assoc) or any(r.action != "refresh" for r in self.records)


def plan_record(
    obj: CompiledObject,
    key: str,
    *,
    row: Mapping[str, Any],
    wh_pk: Any,
    wh_hash: str,
    wh_ts: int | None,
    base: BaseRecord | None,
    crm_record: Mapping[str, Any] | None,
) -> RecordPlan | None:
    """Merge one record. Returns None when nothing (not even state) needs to change."""
    crm_exists = crm_record is not None
    props: Mapping[str, Any] = crm_record.get("properties", {}) if crm_record else {}
    crm_vals = obj.crm_values(props) if crm_exists else {}
    wh_vals = obj.warehouse_values(row)
    crm_ts = parse_hs_timestamp(props.get("hs_lastmodifieddate")) if crm_exists else None
    decisions = [
        merge_field(
            f.spec,
            wh=wh_vals[f.name],
            crm=crm_vals.get(f.name),
            base=base.fields.get(f.name) if base else None,
            crm_exists=crm_exists,
            wh_ts=wh_ts,
            crm_ts=crm_ts,
        )
        for f in obj.fields
    ]
    crm_id = str(crm_record["id"]) if crm_record else None
    rp = RecordPlan(
        object=obj.name,
        key=key,
        action="refresh",
        wh_pk=wh_pk,
        crm_id=crm_id,
        row=dict(row),
        wh_hash=wh_hash,
        wh_ts=wh_ts,
        crm_ts=crm_ts,
        decisions=decisions,
        base=base,
    )
    if not crm_exists:
        rp.action = "create"
    elif rp.crm_writes:
        rp.action = "update"
    elif rp.wh_writes:
        rp.action = "writeback"
    else:
        stale = (
            base is None
            or base.crm_id != crm_id
            or base.wh_hash != wh_hash
            or base.tombstone is not None
            or any(base.fields.get(d.crm_property) != d.new_base() for d in decisions)
        )
        if not stale:
            return None
    return rp

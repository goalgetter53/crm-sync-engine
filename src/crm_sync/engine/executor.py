"""Executor: applies a :class:`Plan` in dependency order with batching, revalidation, CAS
write-back, dead-lettering and per-batch durable state commits."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from crm_sync.clock import Clock
from crm_sync.crm.client import JSON, BatchOutcome, HubSpotClient, WriteFailure
from crm_sync.dsl.loader import CompiledMapping, CompiledObject
from crm_sync.engine.plan import AssocChange, Plan, RecordPlan, plan_record
from crm_sync.state.store import BaseRecord, StateStore, chunked
from crm_sync.values import parse_hs_timestamp
from crm_sync.warehouse.source import WarehouseSource


@dataclass
class ApplyReport:
    created: int = 0
    updated: int = 0
    written_back: int = 0
    refreshed: int = 0
    archived: int = 0
    forgotten: int = 0
    crm_deleted: int = 0
    cas_conflicts: int = 0
    revalidated: int = 0
    dead_lettered: int = 0
    assoc_added: int = 0
    assoc_removed: int = 0
    api_calls: int = 0
    watermarks_committed: bool = False
    failed: set[tuple[str, str]] = field(default_factory=set)

    def as_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if k != "failed"}
        d["failed"] = len(self.failed)
        return d


def _props_for_wire(writes: Mapping[str, str | None]) -> dict[str, str]:
    # HubSpot clears a property when sent "", there is no null on the wire.
    return {k: ("" if v is None else v) for k, v in writes.items()}


class Executor:
    def __init__(
        self,
        mapping: CompiledMapping,
        warehouse: WarehouseSource,
        crm: HubSpotClient,
        state: StateStore,
        clock: Clock,
    ) -> None:
        self.mapping = mapping
        self.wh = warehouse
        self.crm = crm
        self.state = state
        self.clock = clock

    @property
    def _revalidate(self) -> bool:
        return bool(self.mapping.settings.revalidate_before_write)

    @property
    def _bs(self) -> int:
        return int(self.mapping.settings.batch_size)

    def apply(self, plan: Plan, *, commit_watermarks: bool = True) -> ApplyReport:
        report = ApplyReport()
        calls_before = self.crm.metrics.total_calls
        by_obj: dict[str, list[RecordPlan]] = defaultdict(list)
        for r in plan.records:
            by_obj[r.object].append(r)

        for name in self.mapping.order:
            obj = self.mapping.objects[name]
            recs = by_obj.get(name, [])
            self._creates(obj, [r for r in recs if r.action == "create"], report)
            self._updates(
                obj,
                [r for r in recs if r.action in ("update", "writeback", "refresh")],
                report,
            )
            self._tombstone(obj, [r for r in recs if r.action == "crm_deleted"], "crm_deleted")
            report.crm_deleted += sum(1 for r in recs if r.action == "crm_deleted")

        for name in reversed(self.mapping.order):
            obj = self.mapping.objects[name]
            self._archives(obj, by_obj.get(name, []), report)

        self._associations(plan.assoc, report)

        if commit_watermarks and plan.watermarks:
            self.state.set_watermarks(plan.watermarks)
            report.watermarks_committed = True
        report.api_calls = self.crm.metrics.total_calls - calls_before
        return report

    # -- helpers ----------------------------------------------------------------------------
    def _dlq(
        self, obj: CompiledObject, r: RecordPlan, op: str, error: str, report: ApplyReport
    ) -> None:
        self.state.dlq_add(
            obj.name,
            r.key,
            op,
            error,
            self.clock.now_ms(),
            op_id=r.op_id,
            payload={"crm_id": r.crm_id, "writes": r.crm_writes},
        )
        report.dead_lettered += 1
        report.failed.add((obj.name, r.key))

    def _base_after(
        self,
        obj: CompiledObject,
        r: RecordPlan,
        crm_id: str | None,
        echo: Mapping[str, Any] | None,
        *,
        wh_ok: bool = True,
        new_row: Mapping[str, Any] | None = None,
    ) -> BaseRecord:
        fields = {}
        for d in r.decisions:
            written = None
            if d.write_crm and echo is not None and d.crm_property in echo:
                written = obj.field(d.crm_property).from_crm(echo[d.crm_property], obj.maps)
            fields[d.crm_property] = d.new_base(crm_written=written, wh_ok=wh_ok)
        ts = parse_hs_timestamp(echo.get("hs_lastmodifieddate")) if echo else None
        row = new_row if new_row is not None else (r.row or {})
        return BaseRecord(
            key=r.key,
            wh_pk=None if r.wh_pk is None else str(r.wh_pk),
            crm_id=crm_id,
            fields=fields,
            wh_hash=self.wh.row_hash(obj.name, row) if row else r.wh_hash,
            crm_ts=ts if ts is not None else r.crm_ts,
        )

    def _replan(self, obj: CompiledObject, r: RecordPlan, rec: JSON) -> RecordPlan | None:
        assert r.row is not None
        assert r.wh_hash is not None
        return plan_record(
            obj,
            r.key,
            row=r.row,
            wh_pk=r.wh_pk,
            wh_hash=r.wh_hash,
            wh_ts=r.wh_ts,
            base=r.base,
            crm_record=rec,
        )

    # -- creates ----------------------------------------------------------------------------
    def _creates(
        self, obj: CompiledObject, recs: Sequence[RecordPlan], report: ApplyReport
    ) -> None:
        match = obj.spec.match.crm
        for chunk_ in chunked(list(recs), self._bs):
            chunk = list(chunk_)
            converted: list[RecordPlan] = []
            if self._revalidate:
                # Someone may have created the record since we planned; never blind-upsert over
                # fill_if_empty / never_overwrite fields.
                found: dict[str, JSON] = {}
                for rec in self.crm.batch_read(
                    obj.crm_object, [r.key for r in chunk], obj.crm_properties, id_property=match
                ):
                    k = obj.key_from_crm(rec.get("properties", {}))
                    if k is not None:
                        found[k] = rec
                still = []
                for r in chunk:
                    if r.key in found:
                        report.revalidated += 1
                        rp = self._replan(obj, r, found[r.key])
                        if rp is not None:
                            converted.append(rp)
                    else:
                        still.append(r)
                chunk = still
            inputs = [
                {
                    "idProperty": match,
                    "id": r.key,
                    "properties": {
                        **{k: v for k, v in r.crm_writes.items() if v is not None},
                        match: r.key,
                    },
                    "objectWriteTraceId": r.op_id,
                }
                for r in chunk
            ]
            outcome = self.crm.batch_upsert(obj.crm_object, inputs) if inputs else BatchOutcome()
            results: dict[str, JSON] = {}
            for res in outcome.results:
                k = obj.key_from_crm(res.get("properties", {}))
                if k is not None:
                    results[k] = res
            failures = {str(f.input["id"]): f for f in outcome.failures}
            bases, ok = [], []
            for r in chunk:
                hit = results.get(r.key)
                if hit is None:
                    f = failures.get(r.key)
                    self._dlq(obj, r, "create", _describe(f), report)
                    continue
                bases.append(self._base_after(obj, r, str(hit["id"]), hit.get("properties", {})))
                ok.append(r.key)
                report.created += 1
            self.state.save_base(obj.name, bases, self.clock.now_ms())
            self.state.dlq_resolve(obj.name, ok, self.clock.now_ms())
            if converted:
                self._updates(obj, converted, report, revalidated=True)

    # -- updates / write-backs / refreshes --------------------------------------------------
    def _updates(
        self,
        obj: CompiledObject,
        recs: Sequence[RecordPlan],
        report: ApplyReport,
        *,
        revalidated: bool = False,
    ) -> None:
        for chunk_ in chunked(list(recs), self._bs):
            chunk = list(chunk_)
            gone: list[RecordPlan] = []
            need = [r for r in chunk if r.crm_writes]
            if self._revalidate and need and not revalidated:
                # Narrow the read-modify-write window: re-read right before writing and re-merge,
                # so a rep edit made after planning is merged instead of overwritten.
                fresh = {
                    str(x["id"]): x
                    for x in self.crm.batch_read(
                        obj.crm_object, [r.crm_id for r in need if r.crm_id], obj.crm_properties
                    )
                    if not x.get("archived")
                }
                merged: list[RecordPlan] = []
                for r in chunk:
                    if not r.crm_writes:
                        merged.append(r)
                        continue
                    rec = fresh.get(r.crm_id or "")
                    if rec is None:
                        gone.append(r)
                        continue
                    rp = self._replan(obj, r, rec)
                    if rp is None:
                        continue
                    if rp.crm_writes != r.crm_writes or len(rp.wh_writes) != len(r.wh_writes):
                        report.revalidated += 1
                    merged.append(rp)
                chunk = merged
            self._tombstone(obj, gone, "crm_deleted")
            report.crm_deleted += len(gone)

            send = [r for r in chunk if r.crm_writes]
            inputs = [
                {
                    "id": r.crm_id,
                    "properties": _props_for_wire(r.crm_writes),
                    "objectWriteTraceId": r.op_id,
                }
                for r in send
            ]
            outcome = self.crm.batch_update(obj.crm_object, inputs) if inputs else BatchOutcome()
            res_by_id = {str(x["id"]): x for x in outcome.results}
            fail_by_id = {str(f.input["id"]): f for f in outcome.failures}

            bases: list[BaseRecord] = []
            ok: list[str] = []
            deleted_now: list[RecordPlan] = []
            for r in chunk:
                echo: Mapping[str, Any] | None = None
                if r.crm_writes:
                    f = fail_by_id.get(r.crm_id or "")
                    if f is not None and f.category == "OBJECT_NOT_FOUND":
                        deleted_now.append(r)
                        continue
                    if f is not None or (r.crm_id or "") not in res_by_id:
                        self._dlq(obj, r, "update", _describe(f), report)
                        continue
                    echo = res_by_id[r.crm_id or ""].get("properties", {})
                wh_ok, new_row = True, None
                if r.wh_writes:
                    wh_ok, new_row = self._writeback(obj, r)
                    if wh_ok:
                        report.written_back += 1
                    else:
                        report.cas_conflicts += 1
                bases.append(self._base_after(obj, r, r.crm_id, echo, wh_ok=wh_ok, new_row=new_row))
                ok.append(r.key)
                if r.crm_writes:
                    report.updated += 1
                elif not r.wh_writes:
                    report.refreshed += 1
            self.state.save_base(obj.name, bases, self.clock.now_ms())
            self.state.dlq_resolve(obj.name, ok, self.clock.now_ms())
            self._tombstone(obj, deleted_now, "crm_deleted")
            report.crm_deleted += len(deleted_now)

    def _writeback(self, obj: CompiledObject, r: RecordPlan) -> tuple[bool, dict[str, Any] | None]:
        assert r.row is not None
        changes: dict[str, tuple[Any, Any]] = {}
        for d in r.wh_writes:
            col = obj.field(d.crm_property).spec.warehouse
            assert col is not None
            changes[col] = (r.row.get(col), self.wh.to_column_value(obj.name, col, d.resolved))
        if not self.wh.writeback(obj.name, r.wh_pk, changes):
            # Concurrent warehouse edit: keep the stale row hash so the next run re-merges.
            return False, None
        return True, {**r.row, **{c: new for c, (_old, new) in changes.items()}}

    def _tombstone(self, obj: CompiledObject, recs: Sequence[RecordPlan], kind: str) -> None:
        if not recs:
            return
        bases = []
        for r in recs:
            b = r.base
            bases.append(
                BaseRecord(
                    key=r.key,
                    wh_pk=b.wh_pk if b else None,
                    crm_id=r.crm_id,
                    fields=b.fields if b else {},
                    wh_hash=b.wh_hash if b else None,
                    crm_ts=b.crm_ts if b else None,
                    tombstone=kind,
                )
            )
        self.state.save_base(obj.name, bases, self.clock.now_ms())
        self.state.assoc_forget(obj.name, [r.key for r in recs])
        self.state.dlq_resolve(obj.name, [r.key for r in recs], self.clock.now_ms())

    # -- archives ---------------------------------------------------------------------------
    def _archives(
        self, obj: CompiledObject, recs: Sequence[RecordPlan], report: ApplyReport
    ) -> None:
        forget = [r for r in recs if r.action == "forget"]
        self._tombstone(obj, forget, "wh_deleted")
        report.forgotten += len(forget)
        archive = [r for r in recs if r.action == "archive"]
        for chunk in chunked(archive, self._bs):
            outcome = self.crm.batch_archive(obj.crm_object, [r.crm_id or "" for r in chunk])
            failed = {str(f.input["id"]): f for f in outcome.failures}
            done = []
            for r in chunk:
                f = failed.get(r.crm_id or "")
                if f is not None and f.category != "OBJECT_NOT_FOUND":
                    self._dlq(obj, r, "archive", _describe(f), report)
                else:
                    done.append(r)
            self._tombstone(obj, done, "wh_deleted")
            report.archived += len(done)

    # -- associations -----------------------------------------------------------------------
    def _associations(self, changes: Sequence[AssocChange], report: ApplyReport) -> None:
        if not changes:
            return
        ids: dict[str, dict[str, str]] = {}
        for name in {c.from_object for c in changes} | {c.to_object for c in changes}:
            ids[name] = {
                k: b.crm_id
                for k, b in self.state.load_base(name).items()
                if b.crm_id and b.tombstone is None
            }
        groups: dict[tuple[str, str, str], list[AssocChange]] = defaultdict(list)
        for c in changes:
            groups[(c.from_object, c.to_object, c.op)].append(c)
        for (f_name, t_name, op), items in groups.items():
            f_obj, t_obj = self.mapping.objects[f_name], self.mapping.objects[t_name]
            pairs: list[tuple[str, str]] = []
            keymap: dict[tuple[str, str], AssocChange] = {}
            stale: list[tuple[str, str]] = []
            for c in items:
                if (f_name, c.from_key) in report.failed:
                    continue
                fid, tid = ids[f_name].get(c.from_key), ids[t_name].get(c.to_key)
                if fid is None or tid is None:
                    # An endpoint failed to create in this run (it has its own dead letter) or
                    # is gone: adds are re-planned next run, removes are just forgotten.
                    if op == "remove":
                        stale.append((c.from_key, c.to_key))
                    continue
                pairs.append((fid, tid))
                keymap[(fid, tid)] = c
            if stale:
                self.state.assoc_apply(f_name, t_name, [], stale)
            for chunk in chunked(pairs, self._bs):
                if op == "add":
                    outcome = self.crm.associate(f_obj.crm_object, t_obj.crm_object, chunk)
                else:
                    outcome = self.crm.disassociate(f_obj.crm_object, t_obj.crm_object, chunk)
                bad_from = {str(f.input["from"]["id"]) for f in outcome.failures}
                good = [keymap[p] for p in chunk if p[0] not in bad_from]
                for p in chunk:
                    if p[0] in bad_from:
                        c = keymap[p]
                        self.state.dlq_add(
                            f_name, c.from_key, op, "association write failed", self.clock.now_ms()
                        )
                        report.dead_lettered += 1
                        report.failed.add((f_name, c.from_key))
                pairs_k = [(c.from_key, c.to_key) for c in good]
                if op == "add":
                    self.state.assoc_apply(f_name, t_name, pairs_k, [])
                    report.assoc_added += len(pairs_k)
                else:
                    self.state.assoc_apply(f_name, t_name, [], pairs_k)
                    report.assoc_removed += len(pairs_k)


def _describe(f: WriteFailure | None) -> str:
    if f is None:
        return "missing from batch response"
    return f"{f.status} {f.category}: {f.message}"[:1000]

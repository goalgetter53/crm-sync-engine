"""Planner: change detection on both sides + three-way merge -> :class:`Plan`.

Reads only. Running ``plan`` never mutates the CRM, the warehouse or the sync state.
"""

from __future__ import annotations

from typing import Any

from crm_sync.clock import Clock
from crm_sync.crm.client import JSON, HubSpotClient
from crm_sync.dsl.loader import CompiledMapping, CompiledObject
from crm_sync.engine.plan import Action, AssocChange, Plan, RecordPlan, plan_record
from crm_sync.state.store import BaseRecord, StateStore
from crm_sync.warehouse.source import WarehouseSource


class Planner:
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
        self._ready: dict[str, set[str]] = {}
        self._keys: dict[str, dict[str, Any]] = {}

    def build(
        self, *, full_refresh: bool = False, only_keys: dict[str, set[str]] | None = None
    ) -> Plan:
        calls_before = self.crm.metrics.total_calls
        plan = Plan(warnings=list(self.mapping.warnings))
        self._ready, self._keys = {}, {}
        pull_start = self.clock.now_ms()
        for name in self.mapping.order:
            keys = None if only_keys is None else only_keys.get(name, set())
            self._plan_object(self.mapping.objects[name], plan, full_refresh, keys, pull_start)
        plan.planning_calls = self.crm.metrics.total_calls - calls_before
        return plan

    # ------------------------------------------------------------------------------------------
    def _plan_object(
        self,
        obj: CompiledObject,
        plan: Plan,
        full_refresh: bool,
        only: set[str] | None,
        pull_start: int,
    ) -> None:
        name, spec, st = obj.name, obj.spec, self.mapping.settings
        keys_now, dupes = self.wh.scan_keys(name)
        self._keys[name] = keys_now
        plan.warnings.extend(dupes)
        base = self.state.load_base(name)
        live = {k: b for k, b in base.items() if b.tombstone is None}
        respected_deletes = {k for k, b in base.items() if b.tombstone == "crm_deleted"}

        def wanted(k: str) -> bool:
            return (only is None or k in only) and k not in respected_deletes

        # ---- warehouse side: watermark pull + hash compare ------------------------------------
        rows: dict[str, dict[str, Any]] = {}
        hashes: dict[str, str] = {}
        wh_wm = self.state.get_watermark(name, "warehouse")
        max_ts = wh_wm
        if only is None:
            since = None
            if not full_refresh and wh_wm is not None:
                since = wh_wm - int(st.warehouse_lookback_seconds * 1000)
            pulled = self.wh.fetch_changed(name, since)
        else:
            pulled = self.wh.fetch_by_pks(name, [keys_now[k] for k in only if k in keys_now])
        for row in pulled:
            key = obj.key_from_row(row)
            if key is None or keys_now.get(key) != row.get(spec.primary_key) or not wanted(key):
                continue
            ts = self.wh.row_ts_ms(name, row)
            if ts is not None and only is None:
                max_ts = ts if max_ts is None else max(max_ts, ts)
            h = self.wh.row_hash(name, row)
            b = live.get(key)
            if b is None or b.wh_hash != h or only is not None:
                rows[key], hashes[key] = row, h

        # New keys (or keys resurrected after a warehouse delete) not caught by the watermark.
        missing = [k for k in keys_now if wanted(k) and k not in rows and k not in live]
        self._load_rows(name, obj, missing, keys_now, rows, hashes)

        # ---- CRM side: incremental search on hs_lastmodifieddate ------------------------------
        crm_candidates: set[str] = set()
        crm_wm = self.state.get_watermark(name, "crm")
        lag_ms = int(st.search_lag_seconds * 1000)
        if only is None and live:
            since = 0 if (full_refresh or crm_wm is None) else crm_wm - lag_ms
            by_id = {b.crm_id: k for k, b in live.items() if b.crm_id}
            for rec in self.crm.search_modified_since(spec.crm_object, since, obj.crm_properties):
                key = by_id.get(str(rec["id"]))
                if key is None or key in rows or not wanted(key):
                    continue
                cur = obj.crm_values(rec.get("properties", {}))
                b = live[key]
                # Echo suppression: our own writes come back through search; if the indexed
                # values equal the CRM side of the base there is nothing to do.
                if any(cur[f] != b.fields.get(f, (None, None))[1] for f in cur):
                    crm_candidates.add(key)
        if only is None:
            # Store when this pull started; the lag overlap is subtracted at query time.
            plan.watermarks[(name, "crm")] = max(pull_start, crm_wm or 0)
            if max_ts is not None:
                plan.watermarks[(name, "warehouse")] = max_ts

        # ---- dead letters are retried automatically a bounded number of times -----------------
        retry_keys: set[str] = set()
        if only is None:
            retry_keys = {
                d.record_key for d in self.state.dlq_list(name) if d.attempts <= st.max_auto_retries
            }
        extra = [k for k in (crm_candidates | retry_keys) if k in keys_now and k not in rows]
        self._load_rows(name, obj, [k for k in extra if wanted(k)], keys_now, rows, hashes)

        # ---- consistent reads of current CRM state for every candidate ------------------------
        cands = sorted(rows)
        crm_state = self._read_crm(obj, cands, live)

        gone: set[str] = set()
        for key in cands:
            b = live.get(key)
            row = rows[key]
            current = crm_state.get(key)
            if b is not None and b.crm_id and current is None:
                gone.add(key)
                plan.records.append(
                    RecordPlan(
                        name, key, "crm_deleted", wh_pk=keys_now[key], crm_id=b.crm_id, base=b
                    )
                )
                continue
            rp = plan_record(
                obj,
                key,
                row=row,
                wh_pk=keys_now[key],
                wh_hash=hashes[key],
                wh_ts=self.wh.row_ts_ms(name, row),
                base=b,
                crm_record=current,
            )
            if rp is not None:
                plan.records.append(rp)

        # ---- deletes ----------------------------------------------------------------------------
        deleted = [k for k in sorted(live) if k not in keys_now and (only is None or k in only)]
        for key in deleted:
            b = live[key]
            action: Action = "archive" if (spec.archive_on_delete and b.crm_id) else "forget"
            plan.records.append(RecordPlan(name, key, action, crm_id=b.crm_id, base=b))

        # ---- associations: set diff over a full projection, gated on targets that exist -----
        mine = [r for r in plan.records if r.object == name]
        gone_now = {r.key for r in mine if r.action in ("archive", "forget", "crm_deleted")}
        created_now = {r.key for r in mine if r.action == "create"}
        self._ready[name] = ({k for k, b in live.items() if b.crm_id} | created_now) - gone_now
        if spec.associations:
            refs = self.wh.scan_refs(name)
            sources = self._ready[name] if only is None else self._ready[name] & only
            for a in spec.associations:
                target = self.mapping.objects[a.to]
                targets_ready = self._ready.get(a.to, set())
                have = self.state.assoc_load(name, a.to)
                scope = sources | set(have)
                if only is not None:
                    scope &= only
                for key in sorted(scope):
                    if key in gone_now:
                        continue
                    to_key = target.normalize_key(refs.get(key, {}).get(a.via))
                    if to_key is not None and to_key not in targets_ready:
                        if key in rows and to_key not in self._keys.get(a.to, {}):
                            plan.warnings.append(
                                f"{name}:{key}: {a.via}={to_key!r} references no {a.to} row"
                            )
                        to_key = None
                    desired = {to_key} if (to_key and key in sources) else set()
                    linked = have.get(key, set())
                    for t in sorted(desired - linked):
                        plan.assoc.append(AssocChange(name, a.to, key, t, "add"))
                    for t in sorted(linked - desired):
                        plan.assoc.append(AssocChange(name, a.to, key, t, "remove"))

    def _load_rows(
        self,
        name: str,
        obj: CompiledObject,
        keys: list[str],
        keys_now: dict[str, Any],
        rows: dict[str, dict[str, Any]],
        hashes: dict[str, str],
    ) -> None:
        if not keys:
            return
        for row in self.wh.fetch_by_pks(name, [keys_now[k] for k in keys]):
            key = obj.key_from_row(row)
            if key is not None and keys_now.get(key) == row.get(obj.spec.primary_key):
                rows[key], hashes[key] = row, self.wh.row_hash(name, row)

    def _read_crm(
        self, obj: CompiledObject, keys: list[str], live: dict[str, BaseRecord]
    ) -> dict[str, JSON]:
        """Batch-read candidates: by CRM id when known, else by the unique match property."""
        by_id = {live[k].crm_id: k for k in keys if k in live and live[k].crm_id}
        by_key = [k for k in keys if not (k in live and live[k].crm_id)]
        out: dict[str, JSON] = {}
        props = obj.crm_properties
        for rec in self.crm.batch_read(obj.crm_object, [i for i in by_id if i], props):
            k = by_id.get(str(rec["id"]))
            if k is not None and not rec.get("archived"):
                out[k] = rec
        if by_key:
            for rec in self.crm.batch_read(
                obj.crm_object, by_key, props, id_property=obj.spec.match.crm
            ):
                k2 = obj.key_from_crm(rec.get("properties", {}))
                if k2 is not None and not rec.get("archived"):
                    out[k2] = rec
        return out

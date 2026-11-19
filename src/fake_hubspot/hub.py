"""In-memory fake HubSpot portal with realistic failure behaviour.

Behaviours modelled (see README "Fake server" for the list and what is *not* modelled):

* rolling 10-second burst limit and daily limit -> 429 + ``Retry-After`` + rate-limit headers
* search API: 5 req/s limit, max 200 per page, 400 when paging past 10,000 results,
  and an indexing delay (search is eventually consistent; batch read is consistent)
* batch endpoints: max 100 inputs; a single invalid property value fails the *whole* batch (400)
* batch update/read: unknown ids -> 207 Multi-Status with per-item errors
* property validation: unknown/read-only properties, enumeration options, numbers, dates, emails,
  unique-value conflicts; emails are lower-cased and numbers canonicalised like HubSpot does
* random 429s / 5xx and "applied but the response was lost" (502 after commit) fault injection
* optional ``Idempotency-Key`` response cache (NOT a real HubSpot feature; off by default)
"""

from __future__ import annotations

import json
import random
import re
import threading
import uuid
from collections import Counter, deque
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from crm_sync.clock import Clock, SystemClock
from crm_sync.values import ms_to_iso, parse_hs_timestamp
from fake_hubspot.schema import OBJECT_TYPE_IDS, SCHEMAS

JSON = dict[str, Any]
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
SEARCH_CAP = 10_000
SYSTEM_PROPS = ("hs_object_id", "hs_lastmodifieddate", "createdate")


@dataclass
class FaultConfig:
    requests_per_10s: int | None = None
    search_per_second: int | None = 5
    daily_limit: int | None = None
    p_429: float = 0.0
    p_5xx: float = 0.0
    p_apply_then_5xx: float = 0.0
    retry_after_s: float = 1.0
    search_delay_ms: int = 0
    honor_idempotency_keys: bool = False
    seed: int = 0


@dataclass
class Resp:
    status: int
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class Rec:
    id: str
    props: dict[str, str]
    created: int
    updated: int
    archived: bool = False
    idx_props: dict[str, str] | None = None
    idx_updated: int = 0
    idx_archived: bool = False
    idx_due: int | None = None


class ApiError(Exception):
    def __init__(self, status: int, category: str, message: str, errors: list[JSON] | None = None):
        super().__init__(message)
        self.resp = Resp(
            status,
            {
                "status": "error",
                "message": message,
                "correlationId": str(uuid.uuid4()),
                "category": category,
                **({"errors": errors} if errors else {}),
            },
        )


class FakeHub:
    def __init__(self, clock: Clock | None = None, faults: FaultConfig | None = None) -> None:
        self.clock = clock or SystemClock()
        self.lock = threading.RLock()
        self.hooks: list[Callable[[str, JSON], None]] = []
        self.reset(faults)

    # -- lifecycle --------------------------------------------------------------------------
    def reset(self, faults: FaultConfig | None = None) -> None:
        with self.lock:
            self.faults = faults or FaultConfig()
            self.rng = random.Random(self.faults.seed)
            self.records: dict[str, dict[str, Rec]] = {t: {} for t in SCHEMAS}
            self.unique: dict[tuple[str, str], dict[str, str]] = {}
            self.assocs: set[tuple[str, str, str, str]] = set()
            self.next_id = 1001
            self.calls: Counter[str] = Counter()
            self.stats: Counter[str] = Counter()
            self._window: deque[int] = deque()
            self._search_window: deque[int] = deque()
            self._daily = 0
            self._idem: dict[str, Resp] = {}
            self._pending: set[tuple[str, str]] = set()
            self._index_version = 0
            self._search_cache: dict[str, list[str]] = {}

    def configure(self, **kwargs: Any) -> None:
        with self.lock:
            merged = {**asdict(self.faults), **kwargs}
            self.faults = FaultConfig(**merged)
            self.rng = random.Random(self.faults.seed)

    # -- request gate: auth, hooks, limits, random faults, idempotency ------------------------
    def call(
        self,
        op: str,
        headers: Mapping[str, str],
        handler: Callable[[], Resp],
        *,
        write: bool = False,
        search: bool = False,
        body: JSON | None = None,
    ) -> Resp:
        for hook in list(self.hooks):
            hook(op, body or {})
        with self.lock:
            auth = headers.get("authorization", "")
            if not auth.startswith("Bearer ") or len(auth) <= 7:
                return ApiError(
                    401, "INVALID_AUTHENTICATION", "Authentication credentials not found."
                ).resp
            now = self.clock.now_ms()
            self.calls[op] += 1
            idem = headers.get("idempotency-key") if self.faults.honor_idempotency_keys else None
            if idem and idem in self._idem:
                self.stats["idempotent_replays"] += 1
                return self._idem[idem]
            limited = self._check_limits(now, search)
            if limited is not None:
                self.stats["429"] += 1
                return limited
            if self.rng.random() < self.faults.p_429:
                self.stats["429"] += 1
                return self._rate_limited("TEN_SECONDLY_ROLLING", self.faults.retry_after_s)
            if self.rng.random() < self.faults.p_5xx:
                self.stats["5xx"] += 1
                return Resp(503, {"status": "error", "message": "Service Unavailable"})
            self._daily += 1
            (self._search_window if search else self._window).append(now)
            try:
                resp = handler()
            except ApiError as exc:
                resp = exc.resp
            if not search:
                resp.headers.update(self._rate_headers(now))
            if write and resp.status < 300:
                if idem:
                    self._idem[idem] = resp
                if self.rng.random() < self.faults.p_apply_then_5xx:
                    self.stats["applied_then_5xx"] += 1
                    return Resp(502, {"status": "error", "message": "Bad Gateway"})
            return resp

    def _rate_limited(self, policy: str, retry_after: float) -> Resp:
        ra = f"{retry_after:g}"
        return Resp(
            429,
            {
                "status": "error",
                "message": "You have reached your ten_secondly_rolling limit.",
                "errorType": "RATE_LIMIT",
                "correlationId": str(uuid.uuid4()),
                "policyName": policy,
            },
            {"Retry-After": ra},
        )

    def _check_limits(self, now: int, search: bool) -> Resp | None:
        f = self.faults
        if f.daily_limit is not None and self._daily >= f.daily_limit:
            return self._rate_limited("DAILY", 3600)
        if search and f.search_per_second is not None:
            w = self._search_window
            while w and w[0] <= now - 1000:
                w.popleft()
            if len(w) >= f.search_per_second:
                return self._rate_limited("SECONDLY", max(0.001, (w[0] + 1000 - now) / 1000))
        if not search and f.requests_per_10s is not None:
            w = self._window
            while w and w[0] <= now - 10_000:
                w.popleft()
            if len(w) >= f.requests_per_10s:
                return self._rate_limited(
                    "TEN_SECONDLY_ROLLING", max(0.001, (w[0] + 10_000 - now) / 1000)
                )
        return None

    def _rate_headers(self, now: int) -> dict[str, str]:
        f = self.faults
        h: dict[str, str] = {}
        if f.requests_per_10s is not None:
            h["X-HubSpot-RateLimit-Max"] = str(f.requests_per_10s)
            h["X-HubSpot-RateLimit-Remaining"] = str(max(0, f.requests_per_10s - len(self._window)))
            h["X-HubSpot-RateLimit-Interval-Milliseconds"] = "10000"
        if f.daily_limit is not None:
            h["X-HubSpot-RateLimit-Daily"] = str(f.daily_limit)
            h["X-HubSpot-RateLimit-Daily-Remaining"] = str(max(0, f.daily_limit - self._daily))
        return h

    # -- helpers ----------------------------------------------------------------------------
    def _schema(self, obj: str) -> Mapping[str, Any]:
        if obj not in SCHEMAS:
            raise ApiError(400, "VALIDATION_ERROR", f"Unable to infer object type from: {obj}")
        return SCHEMAS[obj]

    def _normalize(self, obj: str, prop: str, value: Any) -> tuple[str | None, JSON | None]:
        pd = self._schema(obj).get(prop)
        if pd is None:
            return None, {
                "message": f'Property "{prop}" does not exist',
                "code": "PROPERTY_DOESNT_EXIST",
                "context": {"propertyName": [prop]},
            }
        if pd.readonly:
            return None, {
                "message": f'"{prop}" is a read only property; its value cannot be set.',
                "code": "READ_ONLY_VALUE",
                "context": {"propertyName": [prop]},
            }
        if value is None or value == "":
            return None, None
        s = str(value)
        if pd.type == "number":
            try:
                d = Decimal(s)
            except InvalidOperation:
                return None, _invalid(prop, f"{s} was not a valid number.", "INVALID_DECIMAL")
            s = str(int(d)) if d == d.to_integral_value() else format(d.normalize(), "f")
        elif pd.type == "enumeration":
            if s not in pd.options:
                return None, _invalid(
                    prop,
                    f"{s} was not one of the allowed options: {list(pd.options)}",
                    "INVALID_OPTION",
                )
        elif pd.type == "date":
            try:
                s = date.fromisoformat(s[:10]).isoformat()
            except ValueError:
                return None, _invalid(prop, f"{s} is not a valid date.", "INVALID_DATE")
        if pd.email:
            s = s.strip().lower()
            if not _EMAIL.match(s):
                return None, _invalid(prop, f"Email address {s} is invalid", "INVALID_EMAIL")
        return s, None

    def _validate(
        self, obj: str, props: Mapping[str, Any], rec_id: str | None
    ) -> tuple[dict[str, str | None], list[JSON]]:
        out: dict[str, str | None] = {}
        errs: list[JSON] = []
        schema = self._schema(obj)
        for k, v in props.items():
            s, err = self._normalize(obj, k, v)
            if err is not None:
                errs.append(err)
                continue
            if s is not None and schema[k].unique:
                owner = self.unique.get((obj, k), {}).get(s)
                if owner is not None and owner != rec_id:
                    errs.append(
                        _invalid(
                            k,
                            f"A {obj[:-1]} with {k}={s} already exists (id {owner})",
                            "OBJECT_ALREADY_EXISTS",
                        )
                    )
                    continue
            out[k] = s
        return out, errs

    def _apply(self, obj: str, rec: Rec, props: Mapping[str, str | None], now: int) -> bool:
        schema = self._schema(obj)
        changed = False
        for k, v in props.items():
            old = rec.props.get(k)
            if old == v:
                continue
            changed = True
            if schema[k].unique:
                idx = self.unique.setdefault((obj, k), {})
                if old is not None:
                    idx.pop(old, None)
                if v is not None:
                    idx[v] = rec.id
            if v is None:
                rec.props.pop(k, None)
            else:
                rec.props[k] = v
        if changed:
            rec.updated = now
            rec.props["hs_lastmodifieddate"] = ms_to_iso(now)
            self._schedule_index(obj, rec, now)
        return changed

    def _new_record(self, obj: str, now: int) -> Rec:
        rid = str(self.next_id)
        self.next_id += 1
        rec = Rec(
            rid,
            {
                "hs_object_id": rid,
                "createdate": ms_to_iso(now),
                "hs_lastmodifieddate": ms_to_iso(now),
            },
            now,
            now,
        )
        self.records[obj][rid] = rec
        return rec

    def _schedule_index(self, obj: str, rec: Rec, now: int) -> None:
        if self.faults.search_delay_ms <= 0:
            self._index(rec)
        else:
            rec.idx_due = now + self.faults.search_delay_ms
            self._pending.add((obj, rec.id))

    def _index(self, rec: Rec) -> None:
        rec.idx_props = dict(rec.props)
        rec.idx_updated = rec.updated
        rec.idx_archived = rec.archived
        rec.idx_due = None
        self._index_version += 1

    def _flush_index(self, now: int) -> None:
        done = []
        for obj, rid in self._pending:
            rec = self.records[obj].get(rid)
            if rec is None or (rec.idx_due is not None and rec.idx_due <= now):
                if rec is not None:
                    self._index(rec)
                done.append((obj, rid))
        for d in done:
            self._pending.discard(d)

    def _resolve(self, obj: str, ident: str, id_property: str | None) -> Rec | None:
        if id_property and id_property != "hs_object_id":
            pd = self._schema(obj).get(id_property)
            if pd is None or not pd.unique:
                raise ApiError(400, "VALIDATION_ERROR", f"{id_property} is not a unique property")
            norm = ident.strip().lower() if pd.email else ident
            rid = self.unique.get((obj, id_property), {}).get(norm)
            if rid is None:
                return None
            ident = rid
        rec = self.records[obj].get(ident)
        return None if rec is None or rec.archived else rec

    def record_json(self, rec: Rec, props: list[str] | None, *, indexed: bool = False) -> JSON:
        src = (rec.idx_props or {}) if indexed else rec.props
        out: dict[str, str | None]
        if props is None:
            out = dict(src)
        else:
            out = {p: src.get(p) for p in props}
            for p in SYSTEM_PROPS:
                out[p] = src.get(p)
        updated = rec.idx_updated if indexed else rec.updated
        return {
            "id": rec.id,
            "properties": out,
            "createdAt": ms_to_iso(rec.created),
            "updatedAt": ms_to_iso(updated),
            "archived": rec.archived,
        }

    @staticmethod
    def _inputs(body: JSON, limit: int = 100) -> list[JSON]:
        inputs = body.get("inputs")
        if not isinstance(inputs, list):
            raise ApiError(400, "VALIDATION_ERROR", "inputs is required")
        if len(inputs) > limit:
            raise ApiError(
                400, "VALIDATION_ERROR", f"Batch input size {len(inputs)} exceeds limit of {limit}"
            )
        return inputs

    # -- CRM v3 objects ---------------------------------------------------------------------
    def batch_read(self, obj: str, body: JSON) -> Resp:
        inputs = self._inputs(body)
        props = body.get("properties") or None
        id_prop = body.get("idProperty")
        results, missing = [], []
        for inp in inputs:
            rec = self._resolve(obj, str(inp.get("id")), id_prop)
            if rec is None:
                missing.append(str(inp.get("id")))
            else:
                results.append(self.record_json(rec, props))
        return _multi(results, missing, obj, "get")

    def batch_upsert(self, obj: str, body: JSON) -> Resp:
        inputs = self._inputs(body)
        now = self.clock.now_ms()
        prepared: list[tuple[JSON, Rec | None, dict[str, str | None]]] = []
        errors: list[JSON] = []
        seen: set[str] = set()
        for inp in inputs:
            id_prop = inp.get("idProperty")
            if not id_prop:
                raise ApiError(400, "VALIDATION_ERROR", "idProperty is required for upsert")
            rec = self._resolve(obj, str(inp.get("id")), str(id_prop))
            props = {**(inp.get("properties") or {}), str(id_prop): inp.get("id")}
            clean, errs = self._validate(obj, props, rec.id if rec else None)
            key = str(clean.get(str(id_prop)))
            if key in seen:
                raise ApiError(
                    400, "VALIDATION_ERROR", f"Duplicate IDs found in batch input: {key}"
                )
            seen.add(key)
            errors.extend(errs)
            prepared.append((inp, rec, clean))
        if errors:
            raise ApiError(400, "VALIDATION_ERROR", "Property values were not valid", errors)
        results = []
        for inp, existing, clean in prepared:
            rec = existing or self._new_record(obj, now)
            self._apply(obj, rec, clean, now)
            res = self.record_json(rec, None)
            res["new"] = existing is None
            if inp.get("objectWriteTraceId"):
                res["objectWriteTraceId"] = inp["objectWriteTraceId"]
            results.append(res)
        self.stats["records_written"] += len(results)
        return Resp(200, {"status": "COMPLETE", "results": results})

    def batch_update(self, obj: str, body: JSON) -> Resp:
        inputs = self._inputs(body)
        now = self.clock.now_ms()
        prepared, missing, errors = [], [], []
        for inp in inputs:
            rec = self._resolve(obj, str(inp.get("id")), body.get("idProperty"))
            if rec is None:
                missing.append(str(inp.get("id")))
                continue
            clean, errs = self._validate(obj, inp.get("properties") or {}, rec.id)
            errors.extend(errs)
            prepared.append((rec, clean))
        if errors:
            raise ApiError(400, "VALIDATION_ERROR", "Property values were not valid", errors)
        results = []
        for rec, clean in prepared:
            self._apply(obj, rec, clean, now)
            results.append(self.record_json(rec, None))
        self.stats["records_written"] += len(results)
        return _multi(results, missing, obj, "update")

    def batch_archive(self, obj: str, body: JSON) -> Resp:
        now = self.clock.now_ms()
        for inp in self._inputs(body):
            rec = self.records[obj].get(str(inp.get("id")))
            if rec is not None and not rec.archived:
                self._archive(obj, rec, now)
        return Resp(204)

    def _archive(self, obj: str, rec: Rec, now: int) -> None:
        rec.archived = True
        rec.updated = now
        for k, v in rec.props.items():
            if self._schema(obj)[k].unique:
                self.unique.get((obj, k), {}).pop(v, None)
        self.assocs = {
            a
            for a in self.assocs
            if (a[0], a[1]) != (obj, rec.id) and (a[2], a[3]) != (obj, rec.id)
        }
        self._schedule_index(obj, rec, now)

    def create(self, obj: str, body: JSON) -> Resp:
        now = self.clock.now_ms()
        clean, errs = self._validate(obj, body.get("properties") or {}, None)
        if errs:
            if any(e.get("code") == "OBJECT_ALREADY_EXISTS" for e in errs):
                raise ApiError(409, "CONFLICT", errs[0]["message"])
            raise ApiError(400, "VALIDATION_ERROR", "Property values were not valid", errs)
        rec = self._new_record(obj, now)
        self._apply(obj, rec, clean, now)
        self._schedule_index(obj, rec, now)
        self.stats["records_written"] += 1
        return Resp(201, self.record_json(rec, None))

    def get(self, obj: str, ident: str, id_property: str | None, props: list[str] | None) -> Resp:
        rec = self._resolve(obj, ident, id_property)
        if rec is None:
            raise ApiError(404, "OBJECT_NOT_FOUND", "resource not found")
        return Resp(200, self.record_json(rec, props))

    def patch(self, obj: str, ident: str, id_property: str | None, body: JSON) -> Resp:
        rec = self._resolve(obj, ident, id_property)
        if rec is None:
            raise ApiError(404, "OBJECT_NOT_FOUND", "resource not found")
        clean, errs = self._validate(obj, body.get("properties") or {}, rec.id)
        if errs:
            raise ApiError(400, "VALIDATION_ERROR", "Property values were not valid", errs)
        self._apply(obj, rec, clean, self.clock.now_ms())
        self.stats["records_written"] += 1
        return Resp(200, self.record_json(rec, None))

    def delete(self, obj: str, ident: str) -> Resp:
        rec = self.records[obj].get(ident)
        if rec is not None and not rec.archived:
            self._archive(obj, rec, self.clock.now_ms())
        return Resp(204)

    def search(self, obj: str, body: JSON) -> Resp:
        self._schema(obj)
        limit = int(body.get("limit", 10))
        if limit > 200:
            raise ApiError(400, "VALIDATION_ERROR", "limit must be <= 200")
        after = int(body.get("after") or 0)
        if after >= SEARCH_CAP:
            raise ApiError(
                400,
                "VALIDATION_ERROR",
                f"The search API returns at most {SEARCH_CAP} results; cannot page past it",
            )
        self._flush_index(self.clock.now_ms())
        groups = body.get("filterGroups") or []
        sorts = body.get("sorts") or []
        ck = json.dumps([obj, groups, sorts, self._index_version], sort_keys=True)
        ids = self._search_cache.get(ck)
        if ids is None:
            self._search_cache = {}
            matches = [
                r
                for r in self.records[obj].values()
                if r.idx_props is not None and not r.idx_archived and _matches(r, groups)
            ]
            if sorts:
                s0 = sorts[0]
                rev = str(s0.get("direction", "ASCENDING")).upper() == "DESCENDING"
                prop = str(s0.get("propertyName"))
                matches.sort(key=lambda r: (_sort_key(r, prop), int(r.id)), reverse=rev)
            else:
                matches.sort(key=lambda r: int(r.id))
            ids = [r.id for r in matches]
            self._search_cache[ck] = ids
        page_ids = ids[after : after + limit]
        props = body.get("properties") or ["createdate", "hs_lastmodifieddate", "hs_object_id"]
        results = [self.record_json(self.records[obj][i], props, indexed=True) for i in page_ids]
        out: JSON = {"total": len(ids), "results": results}
        if after + limit < len(ids):
            out["paging"] = {"next": {"after": str(after + limit)}}
        return Resp(200, out)

    # -- CRM v4 associations ----------------------------------------------------------------
    def associate(self, from_obj: str, to_obj: str, body: JSON) -> Resp:
        results, missing = [], []
        for inp in self._inputs(body, 2000):
            fid, tid = str(inp["from"]["id"]), str(inp["to"]["id"])
            if (
                self._resolve(from_obj, fid, None) is None
                or self._resolve(to_obj, tid, None) is None
            ):
                missing.append(fid)
                continue
            self.assocs.add((from_obj, fid, to_obj, tid))
            self.assocs.add((to_obj, tid, from_obj, fid))
            results.append(
                {
                    "fromObjectTypeId": OBJECT_TYPE_IDS[from_obj],
                    "fromObjectId": int(fid),
                    "toObjectTypeId": OBJECT_TYPE_IDS[to_obj],
                    "toObjectId": int(tid),
                    "labels": [],
                }
            )
        return _multi(results, missing, from_obj, "associate")

    def disassociate(self, from_obj: str, to_obj: str, body: JSON) -> Resp:
        for inp in self._inputs(body, 2000):
            fid = str(inp["from"]["id"])
            for t in inp.get("to", []):
                tid = str(t["id"])
                self.assocs.discard((from_obj, fid, to_obj, tid))
                self.assocs.discard((to_obj, tid, from_obj, fid))
        return Resp(204)

    def read_assocs(self, from_obj: str, to_obj: str, body: JSON) -> Resp:
        results = []
        for inp in self._inputs(body, 1000):
            fid = str(inp["id"])
            to = sorted(int(a[3]) for a in self.assocs if a[:3] == (from_obj, fid, to_obj))
            results.append(
                {
                    "from": {"id": fid},
                    "to": [
                        {
                            "toObjectId": t,
                            "associationTypes": [
                                {"category": "HUBSPOT_DEFINED", "typeId": 1, "label": None}
                            ],
                        }
                        for t in to
                    ],
                }
            )
        return Resp(200, {"status": "COMPLETE", "results": results})

    def associate_one(self, from_obj: str, fid: str, to_obj: str, tid: str) -> Resp:
        return self.associate(
            from_obj, to_obj, {"inputs": [{"from": {"id": fid}, "to": {"id": tid}}]}
        )

    # -- admin (test/bench control plane; never rate limited) --------------------------------
    def admin_edit(self, obj: str, rid: str, props: Mapping[str, Any]) -> JSON:
        """Simulate a sales rep editing a record in the HubSpot UI."""
        with self.lock:
            rec = self.records[obj][rid]
            clean, errs = self._validate(obj, props, rec.id)
            if errs:
                raise ApiError(400, "VALIDATION_ERROR", "invalid", errs)
            self._apply(obj, rec, clean, self.clock.now_ms())
            return self.record_json(rec, None)

    def admin_archive(self, obj: str, rid: str) -> None:
        with self.lock:
            self._archive(obj, self.records[obj][rid], self.clock.now_ms())

    def live(self, obj: str) -> list[Rec]:
        with self.lock:
            return [r for r in self.records[obj].values() if not r.archived]

    def associations(self, from_obj: str, to_obj: str) -> set[tuple[str, str]]:
        with self.lock:
            return {(a[1], a[3]) for a in self.assocs if a[0] == from_obj and a[2] == to_obj}

    def snapshot_stats(self) -> JSON:
        with self.lock:
            return {
                "calls": dict(sorted(self.calls.items())),
                "total_calls": sum(self.calls.values()),
                "stats": dict(self.stats),
                "records": {t: len(self.live(t)) for t in self.records},
                "associations": len(self.assocs) // 2,
                "faults": asdict(self.faults),
            }


def _invalid(prop: str, message: str, code: str) -> JSON:
    return {"message": message, "code": code, "context": {"propertyName": [prop]}}


def _multi(results: list[JSON], missing: list[str], obj: str, verb: str) -> Resp:
    if not missing:
        return Resp(200, {"status": "COMPLETE", "results": results})
    return Resp(
        207,
        {
            "status": "COMPLETE",
            "results": results,
            "numErrors": len(missing),
            "errors": [
                {
                    "status": "error",
                    "category": "OBJECT_NOT_FOUND",
                    "message": f"Could not {verb} some {obj} objects; they may be deleted.",
                    "context": {"ids": missing},
                }
            ],
        },
    )


def _prop_value(rec: Rec, prop: str) -> Any:
    if prop == "hs_lastmodifieddate":
        return rec.idx_updated
    if prop == "hs_object_id":
        return int(rec.id)
    return (rec.idx_props or {}).get(prop)


def _cmp_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, int | float):
        return float(v)
    s = str(v)
    try:
        return float(s)
    except ValueError:
        pass
    try:
        return float(parse_hs_timestamp(s) or 0)
    except ValueError:
        return s


def _sort_key(rec: Rec, prop: str) -> Any:
    v = _cmp_value(_prop_value(rec, prop))
    return (v is None, v if isinstance(v, float) else 0.0, "" if isinstance(v, float) else str(v))


def _matches(rec: Rec, groups: list[JSON]) -> bool:
    if not groups:
        return True
    return any(all(_filter(rec, f) for f in g.get("filters", [])) for g in groups)


def _filter(rec: Rec, f: JSON) -> bool:
    op = str(f.get("operator", "EQ"))
    raw = _prop_value(rec, str(f.get("propertyName")))
    if op == "HAS_PROPERTY":
        return raw is not None
    if op == "NOT_HAS_PROPERTY":
        return raw is None
    if raw is None:
        return False
    if op == "IN":
        return str(raw) in {str(x) for x in f.get("values", [])}
    a, b = _cmp_value(raw), _cmp_value(f.get("value"))
    if type(a) is not type(b):
        a, b = str(raw), str(f.get("value"))
    try:
        return bool(
            {
                "EQ": a == b,
                "NEQ": a != b,
                "GT": a > b,
                "GTE": a >= b,
                "LT": a < b,
                "LTE": a <= b,
            }[op]
        )
    except KeyError:
        raise ApiError(400, "VALIDATION_ERROR", f"unsupported operator {op}") from None

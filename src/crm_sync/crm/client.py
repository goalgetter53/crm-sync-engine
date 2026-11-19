"""HubSpot CRM v3/v4 client focused on batch endpoints.

Failure model:

* 429 -> global cool-down (Retry-After honoured), retry.
* 5xx / transport errors -> exponential backoff with jitter, retry. Every write we issue is
  idempotent (upsert by unique key, update to absolute values, associate/archive are set
  operations), so retrying a request whose response was lost is safe.
* 207 Multi-Status -> per-item errors mapped back to inputs; successes are kept.
* 400/409/422 on a multi-item batch -> HubSpot rejects the *whole* batch when any one input is
  invalid and does not say which one, so we bisect until the poison inputs are isolated.
* single-item 4xx -> permanent failure, returned to the caller for dead-lettering.
"""

from __future__ import annotations

import hashlib
import random
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx

from crm_sync.clock import Clock
from crm_sync.crm.ratelimit import Kind, RateLimiter
from crm_sync.values import parse_hs_timestamp

JSON = dict[str, Any]
SEARCH_RESULT_CAP = 10_000
RETRYABLE = {429, 500, 502, 503, 504}
BISECTABLE = {400, 409, 422}


class HubSpotError(RuntimeError):
    def __init__(self, status: int, message: str, body: Any = None) -> None:
        super().__init__(f"HubSpot {status}: {message}")
        self.status = status
        self.body = body


class SearchWindowOverflow(RuntimeError):
    """More than 10k records share one lastmodifieddate; incremental search cannot page them."""


@dataclass
class WriteFailure:
    input: JSON
    status: int
    category: str
    message: str


@dataclass
class BatchOutcome:
    results: list[JSON] = field(default_factory=list)
    failures: list[WriteFailure] = field(default_factory=list)

    def extend(self, other: BatchOutcome) -> None:
        self.results.extend(other.results)
        self.failures.extend(other.failures)


@dataclass
class ClientMetrics:
    calls: Counter[str] = field(default_factory=Counter)
    retries: int = 0
    rate_limited: int = 0
    server_errors: int = 0
    transport_errors: int = 0
    bisections: int = 0
    search_restarts: int = 0

    @property
    def total_calls(self) -> int:
        return sum(self.calls.values())

    def snapshot(self) -> dict[str, Any]:
        return {
            "total_calls": self.total_calls,
            "calls": dict(sorted(self.calls.items())),
            "retries": self.retries,
            "rate_limited": self.rate_limited,
            "server_errors": self.server_errors,
            "transport_errors": self.transport_errors,
            "bisections": self.bisections,
            "search_restarts": self.search_restarts,
        }


def _endpoint_label(method: str, path: str) -> str:
    parts = path.strip("/").split("/")
    # /crm/v3/objects/{type}/batch/read -> "v3 batch/read"; /crm/v3/objects/{type}/search -> ...
    if len(parts) >= 4 and parts[2] == "objects":
        rest = "/".join(parts[4:]) or "single"
        if not rest.startswith(("batch", "search")):
            rest = "single"
        return f"{parts[1]} {method} {rest}"
    if len(parts) >= 3 and parts[2] == "associations":
        return f"{parts[1]} assoc {'/'.join(parts[5:])}"
    return f"{method} {path}"


class HubSpotClient:
    def __init__(
        self,
        http: httpx.Client,
        limiter: RateLimiter,
        clock: Clock,
        *,
        token: str | None = None,
        max_retries: int = 6,
        batch_size: int = 100,
        search_page_size: int = 200,
        seed: int | None = None,
    ) -> None:
        self.http = http
        self.limiter = limiter
        self.clock = clock
        self.token = token
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.search_page_size = search_page_size
        self.metrics = ClientMetrics()
        self._rng = random.Random(seed)

    # -- transport --------------------------------------------------------------------------
    def _headers(self, idempotency_key: str | None) -> dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if idempotency_key:
            h["Idempotency-Key"] = idempotency_key
        return h

    def _backoff(self, attempt: int) -> float:
        base: float = min(20.0, 0.25 * 2**attempt)
        return base / 2 + self._rng.random() * base / 2

    def request(
        self,
        method: str,
        path: str,
        body: JSON | None = None,
        *,
        kind: Kind = "general",
        idempotency_key: str | None = None,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        label = _endpoint_label(method, path)
        last: httpx.Response | None = None
        for attempt in range(self.max_retries + 1):
            self.limiter.acquire(kind)
            self.metrics.calls[label] += 1
            try:
                resp = self.http.request(
                    method,
                    path,
                    json=body,
                    params=params,
                    headers=self._headers(idempotency_key),
                )
            except httpx.TransportError:
                self.metrics.transport_errors += 1
                if attempt == self.max_retries:
                    raise
                self.metrics.retries += 1
                self.clock.sleep(self._backoff(attempt))
                continue
            self.limiter.observe_headers(resp.headers)
            last = resp
            if resp.status_code == 429:
                self.metrics.rate_limited += 1
                if attempt == self.max_retries:
                    break
                self.metrics.retries += 1
                ra = resp.headers.get("retry-after")
                retry_after = float(ra) if ra is not None else None
                self.limiter.on_rate_limited(retry_after, attempt)
                continue
            if resp.status_code in RETRYABLE:
                self.metrics.server_errors += 1
                if attempt == self.max_retries:
                    break
                self.metrics.retries += 1
                self.clock.sleep(self._backoff(attempt))
                continue
            return resp
        assert last is not None
        raise HubSpotError(last.status_code, "retries exhausted", _safe_json(last))

    # -- reads ------------------------------------------------------------------------------
    def batch_read(
        self,
        obj: str,
        ids: Sequence[str],
        properties: Sequence[str],
        *,
        id_property: str | None = None,
    ) -> list[JSON]:
        """Consistent read by id (or unique property). Missing ids are simply absent."""
        out: list[JSON] = []
        for i in range(0, len(ids), self.batch_size):
            chunk = ids[i : i + self.batch_size]
            body: JSON = {
                "inputs": [{"id": x} for x in chunk],
                "properties": list(properties),
                "propertiesWithHistory": [],
            }
            if id_property:
                body["idProperty"] = id_property
            resp = self.request("POST", f"/crm/v3/objects/{obj}/batch/read", body)
            if resp.status_code not in (200, 207):
                raise HubSpotError(resp.status_code, "batch read failed", _safe_json(resp))
            out.extend(resp.json().get("results", []))
        return out

    def search_modified_since(
        self, obj: str, since_ms: int, properties: Sequence[str]
    ) -> Iterator[JSON]:
        """Yield records with ``hs_lastmodifieddate >= since_ms``, oldest first.

        The search API refuses to page past 10,000 results for one query, so when we approach
        the cap we restart the query from the last timestamp seen (ascending sort) and dedupe by
        id. Search is eventually consistent; callers overlap windows by the index lag.
        """
        seen: set[str] = set()
        lower = since_ms
        props = sorted({*properties, "hs_lastmodifieddate"})
        page = self.search_page_size
        while True:
            after = 0
            last_ts: int | None = None
            while True:
                body: JSON = {
                    "filterGroups": [
                        {
                            "filters": [
                                {
                                    "propertyName": "hs_lastmodifieddate",
                                    "operator": "GTE",
                                    "value": str(lower),
                                }
                            ]
                        }
                    ],
                    "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}],
                    "properties": props,
                    "limit": page,
                    "after": str(after),
                }
                resp = self.request("POST", f"/crm/v3/objects/{obj}/search", body, kind="search")
                if resp.status_code != 200:
                    raise HubSpotError(resp.status_code, "search failed", _safe_json(resp))
                data = resp.json()
                for rec in data.get("results", []):
                    ts = parse_hs_timestamp(rec.get("properties", {}).get("hs_lastmodifieddate"))
                    if ts is not None:
                        last_ts = ts
                    if rec["id"] not in seen:
                        seen.add(rec["id"])
                        yield rec
                nxt = data.get("paging", {}).get("next", {}).get("after")
                if not nxt:
                    return
                after = int(nxt)
                if after + page > SEARCH_RESULT_CAP:
                    break
            if last_ts is None or last_ts <= lower:
                raise SearchWindowOverflow(
                    f"{obj}: >{SEARCH_RESULT_CAP} records modified at the same millisecond; "
                    "run with --full-refresh"
                )
            lower = last_ts
            self.metrics.search_restarts += 1

    # -- writes -----------------------------------------------------------------------------
    def _write_chunk(
        self, path: str, chunk: list[JSON], ident: Callable[[JSON], str]
    ) -> BatchOutcome:
        key = hashlib.sha256(
            (
                path
                + "|"
                + "|".join(ident(x) + ":" + str(x.get("objectWriteTraceId", "")) for x in chunk)
            ).encode()
        ).hexdigest()[:32]
        resp = self.request("POST", path, {"inputs": chunk}, idempotency_key=key)
        status = resp.status_code
        if status in (200, 201, 204):
            return BatchOutcome(
                results=_safe_json(resp).get("results", []) if status != 204 else []
            )
        if status == 207:
            data = _safe_json(resp)
            failed: dict[str, JSON] = {}
            for err in data.get("errors", []):
                ctx = err.get("context", {}) or {}
                for v in [*ctx.get("ids", []), *ctx.get("objectWriteTraceId", [])]:
                    failed[str(v)] = err
            outcome = BatchOutcome(results=data.get("results", []))
            for inp in chunk:
                err = failed.get(ident(inp)) or failed.get(str(inp.get("objectWriteTraceId")))
                if err is not None:
                    outcome.failures.append(
                        WriteFailure(inp, 207, err.get("category", "ERROR"), err.get("message", ""))
                    )
            return outcome
        if status in BISECTABLE and len(chunk) > 1:
            self.metrics.bisections += 1
            mid = len(chunk) // 2
            out = self._write_chunk(path, chunk[:mid], ident)
            out.extend(self._write_chunk(path, chunk[mid:], ident))
            return out
        data = _safe_json(resp)
        detail = "; ".join(str(e.get("message", "")) for e in data.get("errors", [])[:3])
        message = str(data.get("message", resp.text[:300])) + (f": {detail}" if detail else "")
        return BatchOutcome(
            failures=[
                WriteFailure(inp, status, str(data.get("category", "ERROR")), message)
                for inp in chunk
            ]
        )

    def _write(
        self, path: str, inputs: Sequence[JSON], ident: Callable[[JSON], str]
    ) -> BatchOutcome:
        out = BatchOutcome()
        for i in range(0, len(inputs), self.batch_size):
            out.extend(self._write_chunk(path, list(inputs[i : i + self.batch_size]), ident))
        return out

    def batch_upsert(self, obj: str, inputs: Sequence[JSON]) -> BatchOutcome:
        return self._write(f"/crm/v3/objects/{obj}/batch/upsert", inputs, lambda x: str(x["id"]))

    def batch_update(self, obj: str, inputs: Sequence[JSON]) -> BatchOutcome:
        return self._write(f"/crm/v3/objects/{obj}/batch/update", inputs, lambda x: str(x["id"]))

    def batch_archive(self, obj: str, ids: Sequence[str]) -> BatchOutcome:
        return self._write(
            f"/crm/v3/objects/{obj}/batch/archive", [{"id": i} for i in ids], lambda x: str(x["id"])
        )

    def associate(
        self, from_obj: str, to_obj: str, pairs: Sequence[tuple[str, str]]
    ) -> BatchOutcome:
        inputs = [{"from": {"id": a}, "to": {"id": b}} for a, b in pairs]
        return self._write(
            f"/crm/v4/associations/{from_obj}/{to_obj}/batch/associate/default",
            inputs,
            lambda x: str(x["from"]["id"]),
        )

    def disassociate(
        self, from_obj: str, to_obj: str, pairs: Sequence[tuple[str, str]]
    ) -> BatchOutcome:
        inputs = [{"from": {"id": a}, "to": [{"id": b}]} for a, b in pairs]
        return self._write(
            f"/crm/v4/associations/{from_obj}/{to_obj}/batch/archive",
            inputs,
            lambda x: str(x["from"]["id"]),
        )


def _safe_json(resp: httpx.Response) -> JSON:
    try:
        data = resp.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}

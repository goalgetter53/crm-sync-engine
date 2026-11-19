"""HubSpotClient against the fake server: batching, retries, bisection, 207s, search paging."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from crm_sync.clock import ManualClock
from crm_sync.crm.client import HubSpotClient, HubSpotError
from crm_sync.crm.ratelimit import RateLimiter
from fake_hubspot import FakeHub


def client(http: TestClient, clock: ManualClock, **kw: Any) -> HubSpotClient:
    limiter = RateLimiter(clock, requests_per_10s=kw.pop("rpm", 1000), search_per_second=100)
    return HubSpotClient(http, limiter, clock, token="t", seed=3, **kw)


def contacts(n: int, start: int = 0) -> list[dict[str, Any]]:
    return [
        {
            "idProperty": "email",
            "id": f"user{i}@example.com",
            "properties": {"firstname": f"U{i}"},
            "objectWriteTraceId": f"t{i}",
        }
        for i in range(start, start + n)
    ]


def test_writes_are_chunked_to_100(http: TestClient, hub: FakeHub, clock: ManualClock) -> None:
    sizes: list[int] = []
    hub.hooks.append(lambda op, body: sizes.append(len(body.get("inputs", []))))
    out = client(http, clock).batch_upsert("contacts", contacts(250))
    assert len(out.results) == 250
    assert sizes == [100, 100, 50]
    assert len(hub.live("contacts")) == 250


def test_batch_read_by_unique_property_skips_missing(
    http: TestClient, hub: FakeHub, clock: ManualClock
) -> None:
    c = client(http, clock)
    c.batch_upsert("contacts", contacts(3))
    got = c.batch_read(
        "contacts", ["USER1@example.com", "nobody@example.com"], ["firstname"], id_property="email"
    )
    assert [r["properties"]["firstname"] for r in got] == ["U1"]


def test_429_retried_after_retry_after(http: TestClient, hub: FakeHub, clock: ManualClock) -> None:
    hub.configure(requests_per_10s=2, retry_after_s=1.0)
    c = client(http, clock)
    for i in range(4):
        c.batch_upsert("contacts", contacts(1, start=i))
    assert c.metrics.rate_limited >= 1
    assert len(hub.live("contacts")) == 4
    assert c.limiter.stats.cooldowns == c.metrics.rate_limited


def test_5xx_retried_with_backoff(http: TestClient, hub: FakeHub, clock: ManualClock) -> None:
    hub.configure(p_5xx=0.5, seed=11)
    c = client(http, clock)
    c.batch_upsert("contacts", contacts(1000))
    assert c.metrics.server_errors > 0
    assert len(hub.live("contacts")) == 1000


def test_retries_exhausted_raises(http: TestClient, hub: FakeHub, clock: ManualClock) -> None:
    hub.configure(p_5xx=1.0)
    with pytest.raises(HubSpotError, match="retries exhausted"):
        client(http, clock, max_retries=2).batch_upsert("contacts", contacts(1))


def test_lost_response_retry_does_not_duplicate(
    http: TestClient, hub: FakeHub, clock: ManualClock
) -> None:
    """Server commits then returns 502; the retry must not create a second record."""
    hub.configure(p_apply_then_5xx=1.0, honor_idempotency_keys=False)
    c = client(http, clock, max_retries=1)
    with pytest.raises(HubSpotError):
        c.batch_upsert("contacts", contacts(5))
    hub.configure(p_apply_then_5xx=0.0)
    c.batch_upsert("contacts", contacts(5))
    assert len(hub.live("contacts")) == 5  # upsert-by-unique-key is idempotent by construction


def test_bisection_isolates_poison_inputs(
    http: TestClient, hub: FakeHub, clock: ManualClock
) -> None:
    inputs = contacts(100)
    inputs[37]["properties"]["lifecyclestage"] = "zombie"
    inputs[81]["id"] = "not-an-email"
    c = client(http, clock)
    out = c.batch_upsert("contacts", inputs)
    assert len(out.results) == 98
    assert sorted(f.input["id"] for f in out.failures) == ["not-an-email", "user37@example.com"]
    assert all(f.status == 400 for f in out.failures)
    assert "zombie" in next(f.message for f in out.failures if "37" in f.input["id"])
    assert 0 < c.metrics.bisections <= 20


def test_207_partial_update(http: TestClient, hub: FakeHub, clock: ManualClock) -> None:
    c = client(http, clock)
    created = c.batch_upsert("contacts", contacts(2)).results
    ids = [r["id"] for r in created]
    out = c.batch_update(
        "contacts",
        [{"id": i, "properties": {"lastname": "X"}} for i in [*ids, "999999"]],
    )
    assert len(out.results) == 2
    assert [(f.input["id"], f.category) for f in out.failures] == [("999999", "OBJECT_NOT_FOUND")]


def test_search_pages_past_the_10k_cap(http: TestClient, hub: FakeHub, clock: ManualClock) -> None:
    c = client(http, clock)
    for i in range(0, 10_500, 100):
        c.batch_upsert("contacts", contacts(100, start=i))
        clock.advance(1)  # each batch shares one lastmodifieddate, like the real API
    seen = list(c.search_modified_since("contacts", 0, ["email"]))
    assert len(seen) == 10_500
    assert len({r["id"] for r in seen}) == 10_500
    assert c.metrics.search_restarts >= 1


def test_search_is_eventually_consistent(
    http: TestClient, hub: FakeHub, clock: ManualClock
) -> None:
    hub.configure(search_delay_ms=5_000)
    c = client(http, clock)
    c.batch_upsert("contacts", contacts(3))
    assert list(c.search_modified_since("contacts", 0, ["email"])) == []
    assert len(c.batch_read("contacts", ["user0@example.com"], ["email"], id_property="email")) == 1
    clock.advance(5_000)
    assert len(list(c.search_modified_since("contacts", 0, ["email"]))) == 3


def test_requests_without_token_are_rejected(http: TestClient) -> None:
    assert http.post("/crm/v3/objects/contacts/batch/read", json={"inputs": []}).status_code == 401

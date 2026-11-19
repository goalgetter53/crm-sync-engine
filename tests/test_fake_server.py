"""The fake server must behave like HubSpot where it matters, or the integration tests lie."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from crm_sync.clock import ManualClock
from fake_hubspot import FakeHub

AUTH = {"Authorization": "Bearer test"}


def post(http: TestClient, path: str, body: dict[str, Any]) -> Any:
    return http.post(path, json=body, headers=AUTH)


def up(email: str, **props: str) -> dict[str, Any]:
    return {"idProperty": "email", "id": email, "properties": props}


def test_batch_over_100_rejected(http: TestClient) -> None:
    r = post(
        http,
        "/crm/v3/objects/contacts/batch/upsert",
        {"inputs": [up(f"u{i}@x.io") for i in range(101)]},
    )
    assert r.status_code == 400
    assert "exceeds limit of 100" in r.json()["message"]


def test_one_invalid_value_fails_the_whole_batch(http: TestClient, hub: FakeHub) -> None:
    r = post(
        http,
        "/crm/v3/objects/contacts/batch/upsert",
        {"inputs": [up("a@x.io"), up("b@x.io", lifecyclestage="zombie")]},
    )
    assert r.status_code == 400
    body = r.json()
    assert body["category"] == "VALIDATION_ERROR"
    assert body["errors"][0]["code"] == "INVALID_OPTION"
    assert hub.live("contacts") == []


def test_unknown_and_read_only_properties(http: TestClient) -> None:
    r = post(
        http,
        "/crm/v3/objects/contacts/batch/upsert",
        {"inputs": [up("a@x.io", favourite_colour="red", hs_object_id="1")]},
    )
    codes = {e["code"] for e in r.json()["errors"]}
    assert codes == {"PROPERTY_DOESNT_EXIST", "READ_ONLY_VALUE"}


def test_values_are_normalized_like_hubspot(http: TestClient) -> None:
    r = post(
        http,
        "/crm/v3/objects/companies/batch/upsert",
        {
            "inputs": [
                {
                    "idProperty": "warehouse_id",
                    "id": "7",
                    "properties": {"numberofemployees": "120.00"},
                }
            ]
        },
    )
    assert r.json()["results"][0]["properties"]["numberofemployees"] == "120"
    r2 = post(http, "/crm/v3/objects/contacts/batch/upsert", {"inputs": [up("MiXeD@X.io")]})
    assert r2.json()["results"][0]["properties"]["email"] == "mixed@x.io"


def test_search_limits(http: TestClient) -> None:
    assert post(http, "/crm/v3/objects/contacts/search", {"limit": 201}).status_code == 400
    r = post(http, "/crm/v3/objects/contacts/search", {"limit": 100, "after": "10000"})
    assert r.status_code == 400
    assert "10000" in r.json()["message"]


def test_rate_limit_window_returns_429_with_headers(http: TestClient, hub: FakeHub) -> None:
    hub.configure(requests_per_10s=2)
    body = {"inputs": [{"id": "1"}]}
    r1 = post(http, "/crm/v3/objects/contacts/batch/read", body)
    assert r1.headers["X-HubSpot-RateLimit-Remaining"] == "1"
    post(http, "/crm/v3/objects/contacts/batch/read", body)
    r3 = post(http, "/crm/v3/objects/contacts/batch/read", body)
    assert r3.status_code == 429
    assert r3.json()["errorType"] == "RATE_LIMIT"
    assert float(r3.headers["Retry-After"]) > 0


def test_single_create_conflict_and_archive(
    http: TestClient, hub: FakeHub, clock: ManualClock
) -> None:
    r = http.post(
        "/crm/v3/objects/contacts", json={"properties": {"email": "a@x.io"}}, headers=AUTH
    )
    assert r.status_code == 201
    rid = r.json()["id"]
    dup = http.post(
        "/crm/v3/objects/contacts", json={"properties": {"email": "A@x.io"}}, headers=AUTH
    )
    assert dup.status_code == 409
    assert http.delete(f"/crm/v3/objects/contacts/{rid}", headers=AUTH).status_code == 204
    found = http.get("/crm/v3/objects/contacts/a@x.io?idProperty=email", headers=AUTH)
    assert found.status_code == 404
    search = post(http, "/crm/v3/objects/contacts/search", {"limit": 10})
    assert search.json()["total"] == 0


def test_idempotency_key_cache_is_opt_in(http: TestClient, hub: FakeHub) -> None:
    hub.configure(honor_idempotency_keys=True)
    h = {**AUTH, "Idempotency-Key": "k1"}
    body = {"inputs": [up("a@x.io", firstname="A")]}
    http.post("/crm/v3/objects/contacts/batch/upsert", json=body, headers=h)
    http.post("/crm/v3/objects/contacts/batch/upsert", json=body, headers=h)
    assert hub.stats["idempotent_replays"] == 1

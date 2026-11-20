"""Offline tests for the Wikidata loader / replay used by bench/run_real_bench.py, the committed
derived CSVs, the real-companies mapping, and the fake's timeout and partial-batch faults."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from crm_sync.datasets import wikidata as wd
from crm_sync.dsl import load_mapping
from fake_hubspot import FakeHub

ROOT = Path(__file__).resolve().parent.parent
E = wd.ENTITY
AUTH = {"Authorization": "Bearer test"}


def fact_row(item: str, t: str, amount: str, **kw: str) -> dict[str, str]:
    return {
        "item": E + item,
        "s": kw.get("s", f"{item}-{t}-{amount}"),
        "t": t,
        "amount": amount,
        "unit": kw.get("unit", E + "Q199"),
        "rank": kw.get("rank", "http://wikiba.se/ontology#NormalRank"),
    }


def test_flatten_sparql_json() -> None:
    payload = {"results": {"bindings": [{"item": {"type": "uri", "value": E + "Q1"}}]}}
    assert wd.flatten(payload) == [{"item": E + "Q1"}]


def test_parse_facts_filters_deprecated_non_usd_and_duplicates() -> None:
    rows = [
        fact_row("Q1", "2019-01-01T00:00:00Z", "+100"),
        fact_row("Q1", "2019-01-01T00:00:00Z", "+100"),  # same statement via a 2nd class
        fact_row("Q1", "2020-01-01T00:00:00Z", "+999", rank=wd.DEPRECATED),
        fact_row("Q1", "not a date", "+5", s="x"),
    ]
    facts = wd.parse_facts("employees", rows)
    assert [(f.qid, f.on, f.value) for f in facts] == [("Q1", date(2019, 1, 1), 100)]

    rev = [
        fact_row("Q2", "2021-12-31T00:00:00Z", "+1.5E+9", unit=wd.USD, s="a"),
        fact_row("Q2", "2021-12-31T00:00:00Z", "+7", unit=E + "Q4916", s="b"),  # euros: dropped
    ]
    assert [f.value for f in wd.parse_facts("revenue_usd", rev)] == [1_500_000_000]


def test_parse_companies_and_domains() -> None:
    rows = [
        {
            "item": E + "Q312",
            "name": "  Apple   Inc. ",
            "website": "https://www.Apple.com/",
            "inception": "1976-04-01T00:00:00Z",
        }
    ]
    c = wd.parse_companies(rows)["Q312"]
    assert c.name == "Apple Inc."
    assert c.inception == date(1976, 4, 1)
    assert c.dissolved is None
    assert wd.domain_from_url(c.website) == "apple.com"
    assert wd.domain_from_url("example.org/path") == "example.org"
    assert wd.domain_from_url("not a url") is None
    assert wd.domain_from_url(None) is None


def _world() -> tuple[dict[str, wd.Company], list[wd.Fact]]:
    def co(q: str, inc: date | None = None, dis: date | None = None) -> wd.Company:
        return wd.Company(q, q.lower(), f"https://{q.lower()}.example", "X", None, None, inc, dis)

    comps = {
        "Q1": co("Q1", date(2000, 1, 1)),
        "Q2": co("Q2", date(2018, 6, 1)),  # founded later -> insert
        "Q3": co("Q3", None, date(2017, 3, 1)),  # dissolved -> delete
    }
    facts = [
        wd.Fact("Q1", "employees", date(2015, 1, 1), 10, False),
        wd.Fact("Q1", "employees", date(2016, 1, 1), 20, False),
        wd.Fact("Q1", "employees", date(2016, 1, 1), 25, True),  # preferred wins the tie
        wd.Fact("Q1", "revenue_usd", date(2017, 1, 1), 5, False),
    ]
    return comps, facts


def test_snapshot_is_as_of_a_date() -> None:
    comps, facts = _world()
    s15 = wd.snapshot(comps, facts, date(2015, 12, 31))
    assert set(s15) == {"Q1", "Q3"}
    assert s15["Q1"]["employees"] == 10
    assert s15["Q1"]["revenue_usd"] is None
    assert s15["Q1"]["founded_year"] == "2000"
    assert s15["Q1"]["domain"] == "q1.example"
    s16 = wd.snapshot(comps, facts, date(2016, 12, 31))
    assert s16["Q1"]["employees"] == 25


def test_diff_replays_inserts_updates_deletes() -> None:
    comps, facts = _world()
    a = wd.snapshot(comps, facts, date(2015, 12, 31))
    b = wd.snapshot(comps, facts, date(2018, 12, 31))
    d = wd.diff(a, b)
    assert [r["qid"] for r in d.inserts] == ["Q2"]
    assert [r["qid"] for r in d.updates] == ["Q1"]
    assert d.deletes == ["Q3"]
    assert d.changed_fields == {"employees": 1, "revenue_usd": 1}
    assert wd.diff(b, b).counts() == {
        "inserts": 0,
        "updates": 0,
        "deletes": 0,
        "changed_fields": {},
    }


def test_csv_round_trip(tmp_path: Path) -> None:
    comps, facts = _world()
    wd.write_csv(comps, facts, tmp_path)
    comps2, facts2 = wd.read_csv(tmp_path)
    assert comps2 == comps
    assert facts2 == facts


def test_committed_csvs_load_and_are_small() -> None:
    data = ROOT / "bench" / "data"
    for name in ("wikidata_companies.csv", "wikidata_company_facts.csv"):
        assert (data / name).stat().st_size < 5_000_000
    comps, facts = wd.read_csv(data)
    assert len(comps) > 1000
    assert {f.prop for f in facts} == {"employees", "revenue_usd"}
    assert all(q.startswith("Q") for q in comps)


def test_real_companies_mapping_is_valid() -> None:
    m = load_mapping(ROOT / "examples" / "mapping_real_companies.yaml")
    assert set(m.objects) == {"companies"}
    obj = m.objects["companies"]
    row: dict[str, Any] = {
        "qid": "Q312",
        "name": "Apple Inc.",
        "domain": " Apple.com ",
        "website": "https://www.apple.com/",
        "country": "United States",
        "hq_city": "Cupertino",
        "industry": "consumer electronics",
        "founded_year": "1976",
        "employees": 164000,
        "revenue_usd": 391035000000,
        "updated_at": None,
    }
    v = obj.warehouse_values(row)
    assert v["warehouse_id"] == "Q312"
    assert v["domain"] == "apple.com"
    assert v["numberofemployees"] == "164000"


def test_partial_batch_fault_rejects_one_input_with_207(hub: FakeHub, http: TestClient) -> None:
    hub.configure(p_partial_batch=1.0)
    inputs = [{"idProperty": "warehouse_id", "id": f"Q{i}", "properties": {}} for i in range(5)]
    r = http.post("/crm/v3/objects/companies/batch/upsert", json={"inputs": inputs}, headers=AUTH)
    assert r.status_code == 207
    body = r.json()
    assert len(body["results"]) == 4
    assert body["errors"][0]["category"] != "OBJECT_NOT_FOUND"
    assert len(body["errors"][0]["context"]["ids"]) == 1
    assert len(hub.live("companies")) == 4


def test_timeout_fault_commits_then_delays_the_response(hub: FakeHub) -> None:
    hub.configure(p_timeout=1.0, timeout_delay_s=3.0)
    resp = hub.call(
        "x",
        {"authorization": "Bearer t"},
        lambda: hub.batch_upsert(
            "companies", {"inputs": [{"idProperty": "warehouse_id", "id": "Q1"}]}
        ),
        write=True,
    )
    assert resp.status == 200
    assert resp.delay_s == pytest.approx(3.0)
    assert hub.stats["timeouts"] == 1
    assert len(hub.live("companies")) == 1  # applied even though the client will time out


def test_rate_limit_body_matches_documented_shape(hub: FakeHub, http: TestClient) -> None:
    hub.configure(requests_per_10s=1)
    body = {"inputs": [{"id": "1"}]}
    http.post("/crm/v3/objects/companies/batch/read", json=body, headers=AUTH)
    r = http.post("/crm/v3/objects/companies/batch/read", json=body, headers=AUTH)
    assert r.status_code == 429
    j = r.json()
    assert j["errorType"] == "RATE_LIMIT"
    assert j["policyName"] == "TEN_SECONDLY_ROLLING"
    assert {"status", "message", "correlationId", "requestId"} <= set(j)

from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import text

from crm_sync import demo
from crm_sync.dsl import MappingError
from crm_sync.state import BaseRecord, StateStore
from crm_sync.warehouse import WarehouseSource
from tests.conftest import example_mapping, memory_db


@pytest.fixture
def source() -> WarehouseSource:
    eng = memory_db()
    demo.seed(eng, companies_n=2, contacts_n=14, deals_n=2, now=datetime(2026, 1, 1))
    return WarehouseSource(eng, example_mapping())


def test_scan_keys_normalizes_and_flags_duplicates(source: WarehouseSource) -> None:
    with source.engine.begin() as c:
        c.execute(text("UPDATE contacts SET email = ' Dup@X.io ' WHERE id IN (2, 3)"))
    keys, dupes = source.scan_keys("contacts")
    assert keys["dup@x.io"] == 2
    assert len(dupes) == 1
    assert "pk 3 ignored" in dupes[0]
    assert all(k == k.strip().lower() for k in keys)


def test_watermark_pull_only_returns_newer_rows(source: WarehouseSource) -> None:
    with source.engine.begin() as c:
        c.execute(text("UPDATE contacts SET updated_at = '2026-02-01 00:00:00' WHERE id = 5"))
    since = int(datetime(2026, 1, 15).timestamp() * 1000)
    rows = source.fetch_changed("contacts", since)
    assert [r["id"] for r in rows] == [5]
    assert len(source.fetch_changed("contacts", None)) == 14


def test_row_hash_tracks_only_mapped_columns(source: WarehouseSource) -> None:
    row = source.fetch_by_pks("contacts", [1])[0]
    h = source.row_hash("contacts", row)
    assert source.row_hash("contacts", {**row, "updated_at": datetime(2030, 1, 1)}) == h
    assert source.row_hash("contacts", {**row, "first_name": "Zed"}) != h


def test_writeback_is_compare_and_swap(source: WarehouseSource) -> None:
    row = source.fetch_by_pks("contacts", [1])[0]
    assert source.writeback("contacts", 1, {"phone": (row["phone"], "+15550000000")})
    # Second writer with a stale expectation loses and changes nothing.
    assert not source.writeback("contacts", 1, {"phone": (row["phone"], "+15551111111")})
    assert source.fetch_by_pks("contacts", [1])[0]["phone"] == "+15550000000"


def test_writeback_converts_types(source: WarehouseSource) -> None:
    assert source.to_column_value("companies", "employees", "120") == 120
    assert source.to_column_value("contacts", "phone", None) is None


def test_missing_columns_are_reported() -> None:
    eng = memory_db()
    with eng.begin() as c:
        c.execute(text("CREATE TABLE companies (id INTEGER PRIMARY KEY, name TEXT)"))
        c.execute(text("CREATE TABLE contacts (id INTEGER PRIMARY KEY, email TEXT)"))
        c.execute(text("CREATE TABLE deals (id INTEGER PRIMARY KEY)"))
    with pytest.raises(MappingError) as exc:
        WarehouseSource(eng, example_mapping())
    assert any("'domain' not found in table 'companies'" in e for e in exc.value.errors)


def test_state_round_trip_and_dead_letters() -> None:
    st = StateStore(memory_db())
    rec = BaseRecord("k", "1", "101", {"f": ("a", None)}, "h", 5)
    st.save_base("contacts", [rec], 1)
    st.save_base("contacts", [rec], 2)  # upsert, not duplicate
    assert st.load_base("contacts") == {"k": rec}
    st.set_watermarks({("contacts", "crm"): 10})
    st.set_watermarks({("contacts", "crm"): 20})
    assert st.get_watermark("contacts", "crm") == 20
    st.dlq_add("contacts", "k", "create", "boom", 1)
    st.dlq_add("contacts", "k", "create", "boom again", 2)
    [entry] = st.dlq_list()
    assert (entry.attempts, entry.error) == (2, "boom again")
    assert st.dlq_resolve("contacts", ["k"], 3) == 1
    assert st.dlq_list() == []


def test_association_base() -> None:
    st = StateStore(memory_db())
    st.assoc_apply("contacts", "companies", [("a", "1"), ("a", "1"), ("b", "2")], [])
    st.assoc_apply("contacts", "companies", [], [("b", "2")])
    assert st.assoc_load("contacts", "companies") == {"a": {"1"}}
    st.assoc_forget("contacts", ["a"])
    assert st.assoc_load("contacts", "companies") == {}

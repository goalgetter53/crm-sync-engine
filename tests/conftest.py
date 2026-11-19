from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, MetaData, create_engine, select, update
from sqlalchemy.pool import StaticPool

from crm_sync import demo
from crm_sync.clock import ManualClock
from crm_sync.dsl import CompiledMapping, compile_mapping, load_mapping
from crm_sync.engine import ApplyReport, Plan, SyncEngine
from crm_sync.state.store import metadata as state_metadata
from fake_hubspot import FakeHub, FaultConfig, create_app
from fake_hubspot.hub import Rec

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_MAPPING = ROOT / "examples" / "mapping.yaml"


# Set CRM_SYNC_TEST_PG=postgresql+psycopg://... to run the integration suite on Postgres.
PG_URL = os.environ.get("CRM_SYNC_TEST_PG")
_PG_ENGINE: Engine | None = None


def memory_db() -> Engine:
    return create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )


def world_dbs() -> tuple[Engine, Engine]:
    """(warehouse, state) engines: in-memory SQLite by default, a reset Postgres if configured."""
    global _PG_ENGINE
    if not PG_URL:
        return memory_db(), memory_db()
    if _PG_ENGINE is None:
        _PG_ENGINE = create_engine(PG_URL)
    state_metadata.drop_all(_PG_ENGINE)
    return _PG_ENGINE, _PG_ENGINE


def example_mapping(**settings: Any) -> CompiledMapping:
    m = load_mapping(EXAMPLE_MAPPING)
    if not settings:
        return m
    data = m.config.model_dump()
    data["settings"].update(settings)
    return compile_mapping(data)


class World:
    """A fake HubSpot + an in-memory warehouse + a sync engine, all on one manual clock."""

    def __init__(
        self,
        *,
        companies: int = 3,
        contacts: int = 10,
        deals: int = 3,
        bad_rows: int = 0,
        faults: FaultConfig | None = None,
        mapping: CompiledMapping | None = None,
        seed: bool = True,
    ) -> None:
        self.clock = ManualClock()
        self.hub = FakeHub(self.clock, faults or FaultConfig(search_per_second=None))
        self.http = TestClient(create_app(self.hub))
        self.wh, self.state = world_dbs()
        if seed:
            demo.seed(
                self.wh,
                companies_n=companies,
                contacts_n=contacts,
                deals_n=deals,
                bad_rows=bad_rows,
                now=self.now_dt(),
            )
        self.mapping = mapping or example_mapping()
        self.engine = self.new_engine()
        self.md = MetaData()
        self.md.reflect(self.wh, only=["companies", "contacts", "deals"])

    def new_engine(self) -> SyncEngine:
        return SyncEngine(
            self.mapping,
            warehouse=self.wh,
            state=self.state,
            http=self.http,
            clock=self.clock,
            token="test-token",
            seed=1,
        )

    # -- time -------------------------------------------------------------------------------
    def now_dt(self) -> datetime:
        return datetime.fromtimestamp(self.clock.now_ms() / 1000, UTC).replace(tzinfo=None)

    def tick(self, ms: int = 1000) -> None:
        self.clock.advance(ms)

    # -- sync -------------------------------------------------------------------------------
    def settle(self) -> None:
        """Wait out the search-index lag window (the documented quiescence condition)."""
        self.tick(int(self.mapping.settings.search_lag_seconds * 1000) + 1000)

    def sync(self, *, full_refresh: bool = False) -> tuple[Plan, ApplyReport]:
        self.settle()
        return self.engine.apply(full_refresh=full_refresh)

    def plan(self, *, full_refresh: bool = False) -> Plan:
        self.settle()
        return self.engine.plan(full_refresh=full_refresh)

    # -- CRM side ---------------------------------------------------------------------------
    def crm(self, name: str) -> dict[str, Rec]:
        obj = self.mapping.objects[name]
        out: dict[str, Rec] = {}
        for rec in self.hub.live(obj.crm_object):
            key = obj.key_from_crm(rec.props)
            assert key is not None
            assert key not in out, f"duplicate CRM record for {name}:{key}"
            out[key] = rec
        return out

    def edit_crm(self, name: str, key: str, **props: Any) -> None:
        self.tick(5)
        rec = self.crm(name)[key]
        self.hub.admin_edit(self.mapping.objects[name].crm_object, rec.id, props)

    # -- warehouse side ---------------------------------------------------------------------
    def row(self, table: str, pk: Any) -> dict[str, Any]:
        t = self.md.tables[table]
        with self.wh.connect() as c:
            r = c.execute(select(t).where(t.c.id == pk)).mappings().first()
            assert r is not None
            return dict(r)

    def edit_wh(self, table: str, pk: Any, *, touch: bool = True, **cols: Any) -> None:
        self.tick(5)
        t = self.md.tables[table]
        values = dict(cols)
        if touch:
            values["updated_at"] = self.now_dt()
        with self.wh.begin() as c:
            c.execute(update(t).where(t.c.id == pk).values(**values))

    def delete_wh(self, table: str, pk: Any) -> None:
        t = self.md.tables[table]
        with self.wh.begin() as c:
            c.execute(t.delete().where(t.c.id == pk))

    def contact_key(self, pk: int) -> str:
        return str(self.row("contacts", pk)["email"]).strip().lower()


@pytest.fixture
def world() -> World:
    return World()


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def hub(clock: ManualClock) -> FakeHub:
    return FakeHub(clock, FaultConfig(search_per_second=None))


@pytest.fixture
def http(hub: FakeHub) -> TestClient:
    return TestClient(create_app(hub))

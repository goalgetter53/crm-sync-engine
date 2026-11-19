"""Composition root: wires warehouse, state, HubSpot client, planner and executor together."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from sqlalchemy import Engine

from crm_sync.clock import Clock, SystemClock
from crm_sync.crm.client import HubSpotClient
from crm_sync.crm.ratelimit import RateLimiter
from crm_sync.dsl.loader import CompiledMapping
from crm_sync.engine.executor import ApplyReport, Executor
from crm_sync.engine.plan import Plan
from crm_sync.engine.planner import Planner
from crm_sync.state.store import DeadLetter, StateStore
from crm_sync.warehouse.source import WarehouseSource


@dataclass
class ReplayResult:
    attempted: int
    resolved: int
    still_failing: int
    report: ApplyReport


class SyncEngine:
    def __init__(
        self,
        mapping: CompiledMapping,
        *,
        warehouse: Engine,
        state: Engine,
        http: httpx.Client,
        clock: Clock | None = None,
        token: str | None = None,
        seed: int | None = None,
    ) -> None:
        self.clock = clock or SystemClock()
        st = mapping.settings
        self.mapping = mapping
        self.limiter = RateLimiter(
            self.clock,
            requests_per_10s=st.requests_per_10s,
            search_per_second=st.search_requests_per_second,
            daily_limit=st.daily_request_limit,
        )
        self.crm = HubSpotClient(
            http,
            self.limiter,
            self.clock,
            token=token,
            max_retries=st.max_retries,
            batch_size=st.batch_size,
            search_page_size=st.search_page_size,
            seed=seed,
        )
        self.warehouse = WarehouseSource(warehouse, mapping)
        self.state = StateStore(state)
        self.planner = Planner(mapping, self.warehouse, self.crm, self.state, self.clock)
        self.executor = Executor(mapping, self.warehouse, self.crm, self.state, self.clock)

    def plan(self, *, full_refresh: bool = False) -> Plan:
        return self.planner.build(full_refresh=full_refresh)

    def apply(self, *, full_refresh: bool = False) -> tuple[Plan, ApplyReport]:
        plan = self.plan(full_refresh=full_refresh)
        return plan, self.executor.apply(plan)

    def dead_letters(self) -> list[DeadLetter]:
        return self.state.dlq_list()

    def replay(self, ids: list[int] | None = None) -> ReplayResult:
        """Re-drive dead letters with *fresh* data (re-plan the keys, then apply).

        Replaying the stored payload verbatim would resurrect stale values; re-planning picks up
        whatever was fixed in the warehouse or mapping since the failure.
        """
        entries = [d for d in self.state.dlq_list() if ids is None or d.id in ids]
        only: dict[str, set[str]] = {}
        for d in entries:
            only.setdefault(d.object, set()).add(d.record_key)
        plan = self.planner.build(only_keys=only)
        report = self.executor.apply(plan, commit_watermarks=False)
        for obj, keys in only.items():
            ok = [k for k in keys if (obj, k) not in report.failed]
            self.state.dlq_resolve(obj, ok, self.clock.now_ms())
        open_ids = {d.id for d in self.state.dlq_list()}
        resolved = sum(1 for d in entries if d.id not in open_ids)
        return ReplayResult(
            attempted=len(entries),
            resolved=resolved,
            still_failing=len(entries) - resolved,
            report=report,
        )

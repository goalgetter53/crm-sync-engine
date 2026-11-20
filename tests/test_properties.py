"""Hypothesis stateful tests: random interleavings of warehouse edits, rep edits in HubSpot,
deletes, inserts, flaky networks and concurrent edits *during* a sync.

Invariants checked after every quiescent sync (no concurrent writers, search index settled):

1. Convergence   - every owned field agrees on both sides (source_wins and bidirectional fields),
                   associations match the warehouse foreign keys, deleted rows are archived.
2. Idempotence   - planning again immediately yields an empty plan (zero API writes).
3. No duplicates - each record key maps to exactly one live CRM record.
4. No lost rep edits - a crm_wins edit survives until the warehouse changes that field *after*
                   the edit has been synced; a non-empty fill_if_empty value is never clobbered.
                   This holds for edits made concurrently with a sync, except an edit landing in
                   the documented window between the revalidation read and the batch write.
"""

from __future__ import annotations

import os
from typing import Any

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from crm_sync.dsl.functions import _e164
from fake_hubspot import FaultConfig
from tests.conftest import World

PKS = st.integers(1, 5)
WH_LIFECYCLE = st.sampled_from(["lead", "mql", "sql", "opportunity", "customer"])
CRM_LIFECYCLE = st.sampled_from(["lead", "customer", "evangelist", "other"])
PHONES = st.sampled_from(["(212) 555-0100", "212.555.0101", "+1 212 555 0102", None])
NAMES = st.sampled_from(["Ann", "Bo", "Cy", "Di"])
TITLES = st.sampled_from(["CTO", "Buyer", "Analyst"])


class SyncMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        faults = FaultConfig(
            search_per_second=None,
            search_delay_ms=5_000,
            p_429=0.05,
            p_5xx=0.03,
            p_apply_then_5xx=0.05,
            seed=1,
        )
        self.w = World(companies=2, contacts=5, deals=2, faults=faults)
        self.next_pk = 100
        self.deleted: set[int] = set()
        self.deleted_keys: set[str] = set()
        self.crm_deleted: set[str] = set()
        self.syncs = 0
        self.rep_lifecycle: dict[str, tuple[str, int]] = {}
        self.rep_title: dict[str, str] = {}
        self.synced_once = False

    # -- helpers ----------------------------------------------------------------------------
    def _pks(self) -> list[int]:
        return [p for p in [*range(1, 6), *range(100, self.next_pk)] if p not in self.deleted]

    def _crm_rec(self, pk: int) -> Any:
        if pk in self.deleted:
            return None
        return self.w.crm("contacts").get(self.w.contact_key(pk))

    def _rep_edit(self, pk: int, **props: Any) -> None:
        rec = self._crm_rec(pk)
        if rec is None:
            return
        self.w.tick(5)
        self.w.hub.admin_edit("contacts", rec.id, props)
        key = self.w.contact_key(pk)
        # Setting the value the record already has is not a new edit (HubSpot records no change),
        # so it must not shield the field from a later warehouse change.
        if "lifecyclestage" in props and rec.props.get("lifecyclestage") != props["lifecyclestage"]:
            self.rep_lifecycle[key] = (props["lifecyclestage"], self.syncs)
        if props.get("jobtitle"):
            self.rep_title[key] = props["jobtitle"]
        elif "jobtitle" in props:
            self.rep_title.pop(key, None)

    # -- warehouse rules --------------------------------------------------------------------
    @rule(pk=PKS, name=NAMES)
    def wh_first_name(self, pk: int, name: str) -> None:
        if pk not in self.deleted:
            self.w.edit_wh("contacts", pk, first_name=name)

    @rule(pk=PKS, phone=PHONES)
    def wh_phone(self, pk: int, phone: str | None) -> None:
        if pk not in self.deleted:
            self.w.edit_wh("contacts", pk, phone=phone)

    @rule(pk=PKS, stage=WH_LIFECYCLE)
    def wh_lifecycle(self, pk: int, stage: str) -> None:
        if pk in self.deleted:
            return
        key = self.w.contact_key(pk)
        if key in self.rep_lifecycle and self.rep_lifecycle[key][1] < self.syncs:
            del self.rep_lifecycle[key]  # rep edit already synced; a newer warehouse change may win
        self.w.edit_wh("contacts", pk, lifecycle_stage=stage)

    @rule(pk=PKS, title=TITLES)
    def wh_title(self, pk: int, title: str) -> None:
        if pk not in self.deleted:
            self.w.edit_wh("contacts", pk, job_title=title)

    @rule(pk=PKS, company=st.integers(1, 2))
    def wh_company(self, pk: int, company: int) -> None:
        if pk not in self.deleted:
            self.w.edit_wh("contacts", pk, company_id=company)

    @rule()
    def wh_insert(self) -> None:
        pk = self.next_pk
        self.next_pk += 1
        t = self.w.md.tables["contacts"]
        self.w.tick(5)
        with self.w.wh.begin() as c:
            c.execute(
                t.insert().values(
                    id=pk,
                    email=f"new{pk}@example.io",
                    first_name="New",
                    last_name="Row",
                    phone=None,
                    lifecycle_stage="lead",
                    job_title=None,
                    company_id=1,
                    updated_at=self.w.now_dt(),
                )
            )

    @rule(pk=PKS)
    def wh_delete(self, pk: int) -> None:
        if pk not in self.deleted and len(self._pks()) > 2:
            self.deleted_keys.add(self.w.contact_key(pk))
            self.w.delete_wh("contacts", pk)
            self.deleted.add(pk)

    # -- HubSpot rep rules ------------------------------------------------------------------
    @rule(pk=PKS, stage=CRM_LIFECYCLE)
    def rep_lifecycle_edit(self, pk: int, stage: str) -> None:
        self._rep_edit(pk, lifecyclestage=stage)

    @rule(pk=PKS, name=NAMES)
    def rep_drifts_owned_field(self, pk: int, name: str) -> None:
        self._rep_edit(pk, firstname=name)

    @rule(pk=PKS, phone=PHONES)
    def rep_phone(self, pk: int, phone: str | None) -> None:
        self._rep_edit(pk, phone=phone or "")

    @rule(pk=PKS, title=st.one_of(TITLES, st.just("")))
    def rep_title_edit(self, pk: int, title: str) -> None:
        self._rep_edit(pk, jobtitle=title)

    @rule(pk=PKS)
    def rep_deletes_record(self, pk: int) -> None:
        rec = self._crm_rec(pk)
        if rec is not None and len(self.crm_deleted) < 1:
            key = self.w.contact_key(pk)
            self.w.hub.admin_archive("contacts", rec.id)
            self.crm_deleted.add(key)
            self.rep_lifecycle.pop(key, None)
            self.rep_title.pop(key, None)

    # -- syncs ------------------------------------------------------------------------------
    @rule()
    def sync(self) -> None:
        self.w.sync()
        self.syncs += 1
        self.synced_once = True
        self._check_after_quiescent_sync()

    @precondition(lambda self: self.synced_once)
    @rule(
        n=st.integers(0, 12),
        pk=PKS,
        kind=st.sampled_from(["lifecycle", "title", "wh_name"]),
        value=st.sampled_from(["customer", "evangelist", "CEO", "Zed"]),
    )
    def sync_with_concurrent_edit(self, n: int, pk: int, kind: str, value: str) -> None:
        state = {"seen": 0, "fired": False}

        def hook(op: str, body: dict[str, Any]) -> None:
            if state["fired"]:
                return
            if state["seen"] < n:
                state["seen"] += 1
                return
            if kind == "lifecycle" and op == "contacts batch/update":
                return  # documented window between revalidation read and write
            state["fired"] = True
            if kind == "lifecycle":
                stage = value if value in ("customer", "evangelist") else "other"
                self._rep_edit(pk, lifecyclestage=stage)
            elif kind == "title":
                self._rep_edit(pk, jobtitle=value)
            elif pk not in self.deleted:
                self.w.edit_wh("contacts", pk, first_name=value)

        self.w.hub.hooks.append(hook)
        try:
            self.w.sync()
        finally:
            self.w.hub.hooks.remove(hook)
        self.syncs += 1

    # -- invariants -------------------------------------------------------------------------
    def _check_after_quiescent_sync(self) -> None:
        crm = self.w.crm("contacts")  # raises on duplicate keys
        companies = self.w.crm("companies")
        assoc = self.w.hub.associations("contacts", "companies")
        for pk in self._pks():
            row = self.w.row("contacts", pk)
            key = self.w.contact_key(pk)
            if key in self.crm_deleted:
                assert key not in crm, "CRM deletion must be respected"
                continue
            rec = crm.get(key)
            assert rec is not None, f"missing CRM record for {key}"
            assert rec.props.get("firstname") == row["first_name"], "source_wins diverged"
            assert _e164(rec.props.get("phone")) == _e164(row["phone"]), "bidirectional diverged"
            want = {companies[str(row["company_id"])].id}
            assert {t for f, t in assoc if f == rec.id} == want, "association diverged"
            if key in self.rep_lifecycle:
                assert rec.props.get("lifecyclestage") == self.rep_lifecycle[key][0], "lost edit"
            if key in self.rep_title:
                assert rec.props.get("jobtitle") == self.rep_title[key], "clobbered rep value"
        assert not (self.deleted_keys & set(crm)), "warehouse delete must archive"
        live = [p for p in self._pks() if self.w.contact_key(p) not in self.crm_deleted]
        assert len(crm) == len(live)
        plan = self.w.plan()
        assert plan.is_empty, "second sync must be a no-op"
        self.rep_lifecycle.clear()

    @invariant()
    def never_duplicates(self) -> None:
        self.w.crm("contacts")


SyncMachine.TestCase.settings = settings(
    max_examples=int(os.environ.get("SYNC_EXAMPLES", "25")),
    stateful_step_count=18,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)
TestSyncConvergence = SyncMachine.TestCase

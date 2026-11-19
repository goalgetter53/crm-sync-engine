"""End-to-end: warehouse <-> engine <-> fake HubSpot, one scenario per guarantee."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from crm_sync.engine import render_plan
from fake_hubspot import FaultConfig
from tests.conftest import World, example_mapping


def lifecycle_other_than(current: str) -> str:
    return "customer" if current != "customer" else "lead"


# -- basics ------------------------------------------------------------------------------------
def test_initial_sync_creates_everything_then_is_a_noop(world: World) -> None:
    _plan, report = world.sync()
    assert report.created == 16
    assert {k: len(world.hub.live(k)) for k in ("companies", "contacts", "deals")} == {
        "companies": 3,
        "contacts": 10,
        "deals": 3,
    }
    assert len(world.hub.associations("contacts", "companies")) == 10
    assert len(world.hub.associations("deals", "contacts")) == 3
    assert report.watermarks_committed
    again = world.plan()
    assert again.is_empty, render_plan(again)


def test_messy_source_keys_are_normalized(world: World) -> None:
    world.sync()
    raw = world.row("contacts", 7)["email"]
    assert raw != raw.strip().lower()  # seeded with whitespace + upper case
    assert world.contact_key(7) in world.crm("contacts")


def test_only_changed_fields_are_sent(world: World) -> None:
    world.sync()
    sent: list[dict[str, Any]] = []
    world.hub.hooks.append(
        lambda op, body: sent.extend(body["inputs"]) if op == "contacts batch/update" else None
    )
    world.edit_wh("contacts", 1, first_name="Zed")
    plan, report = world.sync()
    assert [r.crm_writes for r in plan.records if r.action == "update"] == [{"firstname": "Zed"}]
    assert [i["properties"] for i in sent] == [{"firstname": "Zed"}]
    assert world.crm("contacts")[world.contact_key(1)].props["firstname"] == "Zed"
    assert report.updated == 1


# -- conflict policies ---------------------------------------------------------------------------
def test_crm_wins_keeps_rep_edit_until_warehouse_changes(world: World) -> None:
    world.sync()
    key = world.contact_key(1)
    world.edit_crm("contacts", key, lifecyclestage="evangelist")
    world.sync()
    assert world.crm("contacts")[key].props["lifecyclestage"] == "evangelist"
    assert world.plan().is_empty
    new = lifecycle_other_than(world.row("contacts", 1)["lifecycle_stage"])
    world.edit_wh("contacts", 1, lifecycle_stage=new)
    world.sync()
    assert world.crm("contacts")[key].props["lifecyclestage"] == new


def test_source_wins_reverts_crm_drift(world: World) -> None:
    world.sync()
    key = world.contact_key(2)
    world.edit_crm("contacts", key, firstname="Hacked")
    plan, _ = world.sync()
    [rec] = [r for r in plan.records if r.key == key]
    assert any("revert CRM drift" in d.reason for d in rec.decisions)
    assert world.crm("contacts")[key].props["firstname"] == world.row("contacts", 2)["first_name"]


def test_bidirectional_field_writes_back_to_warehouse(world: World) -> None:
    world.sync()
    key = world.contact_key(3)
    world.edit_crm("contacts", key, phone="(212) 555-0199")
    _, report = world.sync()
    assert report.written_back == 1
    assert world.row("contacts", 3)["phone"] == "+12125550199"
    assert world.plan().is_empty


@pytest.mark.parametrize("crm_last", [True, False])
def test_latest_timestamp_wins_resolves_true_conflicts(world: World, crm_last: bool) -> None:
    world.sync()
    key = world.contact_key(4)
    if crm_last:
        world.edit_wh("contacts", 4, phone="(415) 555-0100")
        world.edit_crm("contacts", key, phone="+14155550199")
    else:
        world.edit_crm("contacts", key, phone="+14155550199")
        world.edit_wh("contacts", 4, phone="(415) 555-0100")
    plan, _ = world.sync()
    [rec] = [r for r in plan.records if r.key == key]
    assert [d.crm_property for d in rec.conflicts] == ["phone"]
    winner = "+14155550199" if crm_last else "+14155550100"
    assert world.row("contacts", 4)["phone"].replace("(415) 555-0100", "+14155550100") == winner
    assert world.crm("contacts")[key].props["phone"] == winner


def test_fill_if_empty_never_clobbers_but_fills_gaps(world: World) -> None:
    world.sync()
    key = world.contact_key(5)
    world.edit_crm("contacts", key, jobtitle="Chief Wizard")
    world.edit_wh("contacts", 5, job_title="Intern")
    world.sync()
    assert world.crm("contacts")[key].props["jobtitle"] == "Chief Wizard"
    world.edit_crm("contacts", key, jobtitle="")
    world.sync()
    assert world.crm("contacts")[key].props["jobtitle"] == "Intern"


def test_never_overwrite_only_sets_on_create(world: World) -> None:
    world.sync()
    deal = world.crm("deals")["10000001"]
    original = deal.props["closedate"]
    world.edit_wh("deals", 10000001, close_date=date(2027, 6, 30), name="Big")
    world.sync()
    deal = world.crm("deals")["10000001"]
    assert deal.props["closedate"] == original
    assert deal.props["dealname"] == "Big"


def test_existing_crm_record_is_matched_not_duplicated(world: World) -> None:
    email = world.contact_key(6)
    world.http.post(
        "/crm/v3/objects/contacts",
        json={"properties": {"email": email, "firstname": "Old", "lifecyclestage": "evangelist"}},
        headers={"Authorization": "Bearer x"},
    )
    plan, _ = world.sync()
    rec = world.crm("contacts")[email]  # World.crm asserts there are no duplicates
    assert rec.props["firstname"] == world.row("contacts", 6)["first_name"]
    assert rec.props["lifecyclestage"] == "evangelist"  # crm_wins on first-match conflict
    [rp] = [r for r in plan.records if r.key == email]
    assert {d.crm_property for d in rp.conflicts} >= {"firstname", "lifecyclestage"}


# -- deletes & associations ----------------------------------------------------------------------
def test_warehouse_delete_archives_in_crm(world: World) -> None:
    world.sync()
    key = world.contact_key(2)
    world.delete_wh("contacts", 2)
    plan = world.plan()
    assert "- archive   " + key in render_plan(plan)
    _, report = world.engine.apply()
    assert report.archived == 1
    assert key not in world.crm("contacts")
    assert world.plan().is_empty


def test_crm_deletion_is_respected_not_resurrected(world: World) -> None:
    world.sync()
    key = world.contact_key(3)
    world.hub.admin_archive("contacts", world.crm("contacts")[key].id)
    world.edit_wh("contacts", 3, first_name="Again")
    plan, report = world.sync()
    assert [r.action for r in plan.records if r.key == key] == ["crm_deleted"]
    assert report.crm_deleted == 1
    world.edit_wh("contacts", 3, first_name="Again2")
    world.sync()
    assert key not in world.crm("contacts")


def test_association_follows_foreign_key_change(world: World) -> None:
    world.sync()
    key = world.contact_key(1)
    old = world.row("contacts", 1)["company_id"]
    new = 1 if old != 1 else 2
    world.edit_wh("contacts", 1, company_id=new)
    plan, report = world.sync()
    ops = sorted((a.op, a.to_key) for a in plan.assoc)
    assert ops == [("add", str(new)), ("remove", str(old))]
    cid = world.crm("contacts")[key].id
    comp = world.crm("companies")[str(new)].id
    assert {t for f, t in world.hub.associations("contacts", "companies") if f == cid} == {comp}
    assert (report.assoc_added, report.assoc_removed) == (1, 1)


# -- change detection ----------------------------------------------------------------------------
def test_untouched_updated_at_is_caught_by_full_refresh_hash_scan(world: World) -> None:
    world.sync()
    world.tick(400_000)
    world.edit_wh("contacts", 2, first_name="Bump")  # advances the watermark
    world.sync()
    world.tick(400_000)
    world.edit_wh("contacts", 1, touch=False, first_name="Sneaky")
    assert world.plan().is_empty  # outside the incremental window
    full = world.plan(full_refresh=True)
    assert [r.crm_writes for r in full.records] == [{"firstname": "Sneaky"}]


def test_search_lag_is_covered_by_lookback_window() -> None:
    w = World(faults=FaultConfig(search_per_second=None, search_delay_ms=20_000))
    w.sync()
    key = w.contact_key(1)
    w.edit_crm("contacts", key, firstname="Drifted")
    w.tick(10)
    early, _ = w.engine.apply()  # index has not caught up yet: nothing to see
    assert early.is_empty
    later, _ = w.sync()  # next run's window overlaps the lag
    assert [r.key for r in later.records] == [key]
    assert w.crm("contacts")[key].props["firstname"] == w.row("contacts", 1)["first_name"]


# -- failures ------------------------------------------------------------------------------------
def test_poison_record_dead_lettered_retried_then_replayed() -> None:
    w = World(bad_rows=1)
    _, report = w.sync()
    assert report.dead_lettered == 1
    assert report.created == 15
    [dl] = w.engine.dead_letters()
    assert "zombie" in dl.error
    w.sync()
    assert w.engine.dead_letters()[0].attempts == 2  # retried automatically
    w.edit_wh("contacts", 1, lifecycle_stage="lead")
    res = w.engine.replay()
    assert (res.attempted, res.resolved) == (1, 1)
    assert w.contact_key(1) in w.crm("contacts")
    assert w.engine.dead_letters() == []
    assert w.plan().is_empty


def test_crash_mid_apply_then_rerun_converges_without_duplicates() -> None:
    w = World(contacts=250)
    seen = {"n": 0}

    def crash(op: str, body: dict[str, Any]) -> None:
        if op == "contacts batch/upsert":
            seen["n"] += 1
            if seen["n"] == 2:
                raise RuntimeError("process killed")

    w.hub.hooks.append(crash)
    with pytest.raises(RuntimeError, match="process killed"):
        w.sync()
    assert w.engine.state.get_watermark("contacts", "warehouse") is None  # not committed
    w.hub.hooks.clear()
    w.engine = w.new_engine()
    w.sync()
    assert len(w.crm("contacts")) == 250
    assert w.plan().is_empty


def test_flaky_network_still_converges() -> None:
    faults = FaultConfig(
        search_per_second=None, p_429=0.15, p_5xx=0.1, p_apply_then_5xx=0.1, seed=5
    )
    w = World(contacts=120, faults=faults)
    w.sync()
    w.edit_wh("contacts", 1, first_name="X")
    w.sync()
    m = w.engine.crm.metrics
    assert m.rate_limited > 0
    assert m.server_errors > 0
    assert w.hub.stats["applied_then_5xx"] > 0
    assert len(w.crm("contacts")) == 120
    assert w.crm("contacts")[w.contact_key(1)].props["firstname"] == "X"
    w.hub.configure(p_429=0, p_5xx=0, p_apply_then_5xx=0)
    assert w.plan().is_empty


def test_client_side_scheduler_prevents_429s() -> None:
    w = World(
        contacts=400,
        faults=FaultConfig(requests_per_10s=10, search_per_second=5),
        mapping=example_mapping(requests_per_10s=10),
    )
    t0 = w.clock.now_ms()
    _, report = w.sync()
    elapsed_s = (w.clock.now_ms() - t0) / 1000
    assert w.hub.stats["429"] == 0
    assert elapsed_s >= (report.api_calls - 10) / 1.0  # never faster than 1 req/s sustained


# -- concurrency: no lost updates ----------------------------------------------------------------
def _stale_plan_with_rep_edit(w: World) -> tuple[Any, str, str]:
    w.sync()
    key = w.contact_key(1)
    new = lifecycle_other_than(w.row("contacts", 1)["lifecycle_stage"])
    w.edit_wh("contacts", 1, lifecycle_stage=new, first_name="Planned")
    plan = w.plan()
    w.edit_crm("contacts", key, lifecyclestage="evangelist")  # rep edits after planning
    return plan, key, new


def test_rep_edit_after_planning_is_not_lost(world: World) -> None:
    plan, key, _ = _stale_plan_with_rep_edit(world)
    report = world.engine.executor.apply(plan)
    rec = world.crm("contacts")[key]
    assert rec.props["lifecyclestage"] == "evangelist"  # crm_wins honoured on fresh data
    assert rec.props["firstname"] == "Planned"
    assert report.revalidated == 1


def test_without_revalidation_the_same_race_loses_the_edit() -> None:
    w = World(mapping=example_mapping(revalidate_before_write=False))
    plan, key, new = _stale_plan_with_rep_edit(w)
    w.engine.executor.apply(plan)
    assert w.crm("contacts")[key].props["lifecyclestage"] == new  # documents the race


def test_edit_to_another_field_during_the_write_survives(world: World) -> None:
    world.sync()
    key = world.contact_key(2)
    rid = world.crm("contacts")[key].id

    def rep_types_while_we_write(op: str, body: dict[str, Any]) -> None:
        if op == "contacts batch/update":
            world.hub.admin_edit("contacts", rid, {"jobtitle": "Typed By Rep"})

    world.hub.hooks.append(rep_types_while_we_write)
    world.edit_wh("contacts", 2, first_name="Batch")
    world.sync()
    world.hub.hooks.clear()
    world.sync()
    rec = world.crm("contacts")[key]
    assert (rec.props["firstname"], rec.props["jobtitle"]) == ("Batch", "Typed By Rep")


def test_concurrent_warehouse_edit_wins_over_stale_writeback(world: World) -> None:
    world.sync()
    key = world.contact_key(3)
    world.edit_crm("contacts", key, phone="+13125550001")
    plan = world.plan()
    assert plan.counts()["writeback_fields"] == 1
    world.edit_wh("contacts", 3, phone="+13125550002")  # lands between plan and apply
    report = world.engine.executor.apply(plan)
    assert report.cas_conflicts == 1
    assert world.row("contacts", 3)["phone"] == "+13125550002"
    world.sync()
    assert world.crm("contacts")[key].props["phone"] == "+13125550002"
    assert world.plan().is_empty

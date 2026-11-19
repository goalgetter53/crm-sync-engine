"""Truth table + property-based tests for the field-level three-way merge."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from crm_sync.dsl.models import DIRECTIONS, POLICIES, FieldMapping
from crm_sync.engine.merge import merge_field

VALS = st.sampled_from([None, "a", "b", "c"])
TS = st.one_of(st.none(), st.integers(0, 10))


def spec(policy: str = "source_wins", direction: str = "to_crm") -> FieldMapping:
    return FieldMapping(crm="f", warehouse="f", policy=policy, direction=direction)  # type: ignore[arg-type]


def valid_specs() -> st.SearchStrategy[FieldMapping]:
    return (
        st.tuples(st.sampled_from(POLICIES), st.sampled_from(DIRECTIONS))
        .filter(lambda t: t[1] != "to_warehouse" or t[0] == "crm_wins")
        .map(lambda t: spec(*t))
    )


@pytest.mark.parametrize(
    ("policy", "direction", "wh", "crm", "base", "resolved", "write_crm", "write_wh"),
    [
        # record missing in CRM -> create from warehouse
        ("crm_wins", "to_crm", "a", None, None, "a", True, False),
        # warehouse changed only
        ("crm_wins", "to_crm", "b", "a", ("a", "a"), "b", True, False),
        # rep edited only: crm_wins keeps it, source_wins reverts it
        ("crm_wins", "to_crm", "a", "x", ("a", "a"), "x", False, False),
        ("source_wins", "to_crm", "a", "x", ("a", "a"), "a", True, False),
        # bidirectional: rep edit flows back to the warehouse
        ("source_wins", "bidirectional", "a", "x", ("a", "a"), "x", False, True),
        # true conflict resolved by policy
        ("source_wins", "bidirectional", "b", "x", ("a", "a"), "b", True, False),
        ("crm_wins", "bidirectional", "b", "x", ("a", "a"), "x", False, True),
        # fill_if_empty / never_overwrite guards
        ("fill_if_empty", "to_crm", "b", "x", ("a", "x"), "x", False, False),
        ("fill_if_empty", "to_crm", "b", None, ("a", "x"), "b", True, False),
        ("never_overwrite", "to_crm", "b", None, ("a", "a"), None, False, False),
        # steady-state divergence allowed by crm_wins is not re-flagged
        ("crm_wins", "to_crm", "a", "x", ("a", "x"), "x", False, False),
        # CRM owns the field
        ("crm_wins", "to_warehouse", "a", "x", ("a", "a"), "x", False, True),
    ],
)
def test_truth_table(
    policy: str,
    direction: str,
    wh: str | None,
    crm: str | None,
    base: tuple[str | None, str | None] | None,
    resolved: str | None,
    write_crm: bool,
    write_wh: bool,
) -> None:
    d = merge_field(
        spec(policy, direction),
        wh=wh,
        crm=crm,
        base=base,
        crm_exists=crm is not None or base is not None,
    )
    assert (d.resolved, d.write_crm, d.write_wh) == (resolved, write_crm, write_wh)


def test_latest_timestamp_wins_conflict_uses_record_timestamps() -> None:
    s = spec("latest_timestamp_wins", "bidirectional")
    older_crm = merge_field(s, wh="b", crm="x", base=("a", "a"), crm_exists=True, wh_ts=9, crm_ts=5)
    newer_crm = merge_field(s, wh="b", crm="x", base=("a", "a"), crm_exists=True, wh_ts=5, crm_ts=9)
    tie = merge_field(s, wh="b", crm="x", base=("a", "a"), crm_exists=True, wh_ts=5, crm_ts=5)
    assert (older_crm.resolved, newer_crm.resolved, tie.resolved) == ("b", "x", "b")
    assert older_crm.conflict
    assert newer_crm.conflict


@given(
    s=valid_specs(),
    wh=VALS,
    crm=VALS,
    bw=VALS,
    bc=VALS,
    has_base=st.booleans(),
    exists=st.booleans(),
    wts=TS,
    cts=TS,
)
def test_applying_a_decision_is_idempotent(
    s: FieldMapping,
    wh: str | None,
    crm: str | None,
    bw: str | None,
    bc: str | None,
    has_base: bool,
    exists: bool,
    wts: int | None,
    cts: int | None,
) -> None:
    """After applying the writes, merging again with the new base produces no writes."""
    base = (bw, bc) if has_base else None
    crm_v = crm if exists else None
    d = merge_field(s, wh=wh, crm=crm_v, base=base, crm_exists=exists, wh_ts=wts, crm_ts=cts)
    wh2 = d.resolved if d.write_wh else wh
    crm2 = d.resolved if d.write_crm else crm_v
    d2 = merge_field(s, wh=wh2, crm=crm2, base=d.new_base(), crm_exists=True, wh_ts=wts, crm_ts=cts)
    assert not d2.write_crm
    assert not d2.write_wh


@given(wh=VALS, crm=VALS, bw=VALS, bc=VALS, policy=st.sampled_from(POLICIES))
def test_owned_fields_converge(
    wh: str | None, crm: str | None, bw: str | None, bc: str | None, policy: str
) -> None:
    """Bidirectional fields (any policy) and to_crm/source_wins fields end with both sides equal."""
    for direction in ("bidirectional", "to_crm"):
        if direction == "to_crm" and policy != "source_wins":
            continue
        d = merge_field(spec(policy, direction), wh=wh, crm=crm, base=(bw, bc), crm_exists=True)
        after_wh = d.resolved if d.write_wh else wh
        after_crm = d.resolved if d.write_crm else crm
        assert after_wh == after_crm == d.resolved


@given(wh=VALS, crm=st.sampled_from(["a", "x"]), bw=VALS, bc=VALS)
def test_fill_if_empty_and_never_overwrite_never_clobber(
    wh: str | None, crm: str, bw: str | None, bc: str | None
) -> None:
    for policy in ("fill_if_empty", "never_overwrite"):
        d = merge_field(spec(policy), wh=wh, crm=crm, base=(bw, bc), crm_exists=True)
        assert not d.write_crm
        assert d.resolved == crm


@given(wh=VALS, crm=VALS, bw=VALS, bc=VALS)
def test_crm_wins_never_discards_a_rep_edit(
    wh: str | None, crm: str | None, bw: str | None, bc: str | None
) -> None:
    d = merge_field(spec("crm_wins"), wh=wh, crm=crm, base=(bw, bc), crm_exists=True)
    if crm != bc:  # the rep changed it since the last sync
        assert d.resolved == crm
        assert not d.write_crm


@given(wh=VALS, crm=VALS, bw=VALS, bc=VALS)
def test_conflict_flag_means_both_sides_moved(
    wh: str | None, crm: str | None, bw: str | None, bc: str | None
) -> None:
    d = merge_field(
        spec("crm_wins", "bidirectional"), wh=wh, crm=crm, base=(bw, bc), crm_exists=True
    )
    assert d.conflict == (wh != bw and crm != bc and wh != crm)

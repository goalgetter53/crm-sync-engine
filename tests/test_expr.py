from __future__ import annotations

from decimal import Decimal

import pytest

from crm_sync.dsl.expr import ExprError, compile_expr
from crm_sync.values import canon, ms_to_iso, parse_hs_timestamp

MAPS = {"lifecycle": {"mql": "marketingqualifiedlead"}}


def ev(src: str, value: object = None, row: dict[str, object] | None = None) -> object:
    return compile_expr(src, MAPS)(value, row or {}, MAPS)


def test_normalizer_chain_evaluates_and_is_detected() -> None:
    e = compile_expr("lower(trim(value))")
    assert e(" Alice@X.COM ", {}, {}) == "alice@x.com"
    assert e.is_normalizer_chain
    assert not compile_expr("concat(value, 'x')").is_normalizer_chain


def test_row_references_and_concat_skip_nulls() -> None:
    e = compile_expr("concat(row.first, ' ', row.last)")
    assert e.columns == frozenset({"first", "last"})
    assert e(None, {"first": "Ava", "last": "Kim"}, {}) == "Ava Kim"
    assert e(None, {"first": "Ava", "last": None}, {}) == "Ava "


def test_lookup_maps_values_and_passes_through_unknown() -> None:
    assert ev("lookup(value, 'lifecycle')", "mql") == "marketingqualifiedlead"
    assert ev("lookup(value, 'lifecycle')", "zombie") == "zombie"
    assert ev("lookup(value, 'lifecycle', 'other')", "zombie") == "other"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("(555) 123-4567", "+15551234567"),
        ("555.123.4567", "+15551234567"),
        ("+44 20 7946 0958", "+442079460958"),
        ("1-555-123-4567", "+15551234567"),
        ("n/a", None),
    ],
)
def test_e164(raw: str, expected: str | None) -> None:
    assert ev("e164(value)", raw) == expected
    if expected is not None:  # idempotent: safe for write-back fields
        assert ev("e164(value)", expected) == expected


@pytest.mark.parametrize(
    "src",
    [
        "__import__('os').system('id')",
        "value.__class__",
        "row['x']",
        "1 + 1",
        "lambda: 1",
        "open('/etc/passwd')",
    ],
)
def test_code_injection_is_impossible(src: str) -> None:
    with pytest.raises(ExprError):
        compile_expr(src)


def test_unknown_function_suggests_closest() -> None:
    with pytest.raises(ExprError, match="did you mean 'lower'"):
        compile_expr("lowr(value)")


def test_arity_and_map_checks() -> None:
    with pytest.raises(ExprError, match="takes 1 argument"):
        compile_expr("lower(value, value)")
    with pytest.raises(ExprError, match="undefined map 'nope'"):
        compile_expr("lookup(value, 'nope')", MAPS)


def test_depth_and_length_limits() -> None:
    with pytest.raises(ExprError, match="deeper"):
        compile_expr("lower(" * 40 + "value" + ")" * 40)
    with pytest.raises(ExprError, match="longer"):
        compile_expr("concat(" + ",".join(["value"] * 200) + ")")
    with pytest.raises(ExprError, match="empty"):
        compile_expr("   ")


def test_literals() -> None:
    assert ev("'default'") == "default"
    assert ev("default(value, 3)", None) == 3
    assert ev("round(value, 1)", "2.345") == Decimal("2.3")
    assert ev("coalesce(null, '', 'x')") == "x"


def test_canon_value_space() -> None:
    assert canon(Decimal("1500.00")) == "1500"
    assert canon(Decimal("12.50")) == "12.5"
    assert canon(42.0) == "42"
    assert canon(True) == "true"
    assert canon("") is None
    assert canon(None) is None


def test_timestamp_parsing_round_trip() -> None:
    ms = 1_767_225_600_123
    assert parse_hs_timestamp(ms_to_iso(ms)) == ms
    assert parse_hs_timestamp(str(ms)) == ms
    assert parse_hs_timestamp("") is None

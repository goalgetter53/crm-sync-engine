"""Registered transform functions. The expression language can only call these (no eval)."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from crm_sync.values import canon

Maps = Mapping[str, Mapping[str, str]]


@dataclass(frozen=True)
class Function:
    name: str
    impl: Callable[..., Any]
    min_args: int
    max_args: int | None
    doc: str
    # A normalizer is single-argument and idempotent: f(f(x)) == f(x). Only normalizer chains are
    # allowed on fields that write back to the warehouse, otherwise round-trips would flap.
    normalizer: bool = False
    needs_maps: bool = False


def _str(v: Any) -> str | None:
    return canon(v)


def _lower(v: Any) -> str | None:
    s = _str(v)
    return None if s is None else s.lower()


def _upper(v: Any) -> str | None:
    s = _str(v)
    return None if s is None else s.upper()


def _trim(v: Any) -> str | None:
    s = _str(v)
    if s is None:
        return None
    s = s.strip()
    return s or None


def _title(v: Any) -> str | None:
    s = _str(v)
    return None if s is None else s.title()


_NON_DIGIT = re.compile(r"\D")


def _e164(v: Any) -> str | None:
    """Best-effort E.164 for NANP numbers; leaves already-international numbers intact."""
    s = _str(v)
    if s is None:
        return None
    digits = _NON_DIGIT.sub("", s)
    if not digits:
        return None
    if s.strip().startswith("+"):
        return "+" + digits
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return "+" + digits


def _concat(*parts: Any) -> str | None:
    out = "".join(p for p in (_str(x) for x in parts) if p is not None)
    return out or None


def _coalesce(*vals: Any) -> Any:
    for v in vals:
        if canon(v) is not None:
            return v
    return None


def _default(v: Any, fallback: Any) -> Any:
    return v if canon(v) is not None else fallback


def _to_int(v: Any) -> int | None:
    s = _str(v)
    if s is None:
        return None
    try:
        return int(Decimal(s))
    except (InvalidOperation, ValueError):
        return None


def _round(v: Any, places: Any = 0) -> Decimal | None:
    s = _str(v)
    if s is None:
        return None
    try:
        return round(Decimal(s), int(places))
    except (InvalidOperation, ValueError):
        return None


def _domain_from_email(v: Any) -> str | None:
    s = _str(v)
    if s is None or "@" not in s:
        return None
    return s.rsplit("@", 1)[1].strip().lower() or None


def _iso_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = _str(v)
    return None if s is None else s[:10]


def _lookup(v: Any, map_name: Any, fallback: Any = None, *, maps: Maps) -> Any:
    s = _str(v)
    if s is None:
        return None
    table = maps[str(map_name)]
    if s in table:
        return table[s]
    # Unmapped values pass through unless a fallback is given, so bad data surfaces as a CRM
    # validation error (dead-lettered) instead of being silently nulled.
    return fallback if fallback is not None else s


REGISTRY: dict[str, Function] = {
    f.name: f
    for f in [
        Function("lower", _lower, 1, 1, "Lower-case a string.", normalizer=True),
        Function("upper", _upper, 1, 1, "Upper-case a string.", normalizer=True),
        Function("trim", _trim, 1, 1, "Strip surrounding whitespace; blank -> null.", True),
        Function("title", _title, 1, 1, "Title Case.", normalizer=True),
        Function("e164", _e164, 1, 1, "Normalize a phone number to E.164.", normalizer=True),
        Function("concat", _concat, 1, None, "Concatenate values, skipping nulls."),
        Function("coalesce", _coalesce, 1, None, "First non-empty argument."),
        Function("default", _default, 2, 2, "Value, or fallback when empty."),
        Function("to_str", _str, 1, 1, "Canonical string form.", normalizer=True),
        Function("to_int", _to_int, 1, 1, "Integer, or null if unparseable."),
        Function("round", _round, 1, 2, "Round a number to N places."),
        Function("domain_from_email", _domain_from_email, 1, 1, "Domain part of an email."),
        Function("iso_date", _iso_date, 1, 1, "Date as YYYY-MM-DD."),
        Function("lookup", _lookup, 2, 3, "Map a value through a named map.", needs_maps=True),
    ]
}

"""Canonical value space.

HubSpot transports every property value as a string. All comparisons in the engine happen on
canonical strings so that ``Decimal("1500.00")`` in the warehouse equals ``"1500"`` in the CRM.
``None`` and ``""`` are both "empty".
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

CanonValue = str | None


def canon(value: Any) -> CanonValue:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return str(int(value))
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    text = str(value)
    return text if text != "" else None


def parse_hs_timestamp(value: Any) -> int | None:
    """Parse a HubSpot timestamp (ISO-8601 string or epoch-millis) to epoch millis."""
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    text = str(value)
    if text.isdigit():
        return int(text)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def ms_to_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=UTC)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

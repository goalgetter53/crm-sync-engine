"""Warehouse access via SQLAlchemy Core (works on SQLite and Postgres).

Change detection = ``updated_at`` watermark (cheap, incremental) + row-hash snapshot compare
(exact, catches no-op touches). Write-back uses compare-and-swap so a concurrent warehouse edit is
never clobbered.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Column, Engine, MetaData, Table, and_, select, update
from sqlalchemy.sql.elements import ColumnElement

from crm_sync.dsl.loader import CompiledMapping, CompiledObject, MappingError
from crm_sync.state.store import chunked
from crm_sync.values import CanonValue, canon

Row = dict[str, Any]


def _ts_to_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return int(value.timestamp() * 1000)
    if isinstance(value, int | float):
        return int(value)
    text = str(value)
    if text.isdigit():
        return int(text)
    return _ts_to_ms(datetime.fromisoformat(text.replace("Z", "+00:00")))


class WarehouseSource:
    def __init__(self, engine: Engine, mapping: CompiledMapping) -> None:
        self.engine = engine
        self.mapping = mapping
        md = MetaData()
        tables = sorted({o.spec.table for o in mapping.objects.values()})
        md.reflect(engine, only=tables)
        self.tables: dict[str, Table] = {}
        errors: list[str] = []
        for name, obj in mapping.objects.items():
            t = md.tables[obj.spec.table]
            self.tables[name] = t
            needed = set(obj.source_columns) | {obj.spec.updated_at}
            for col in sorted(needed - set(t.c.keys())):
                errors.append(f"objects.{name}: column {col!r} not found in table {t.name!r}")
        if errors:
            raise MappingError(errors)

    def _obj(self, name: str) -> tuple[CompiledObject, Table]:
        return self.mapping.objects[name], self.tables[name]

    def _ms_to_param(self, table: Table, col: str, ms: int) -> Any:
        try:
            py = table.c[col].type.python_type
        except NotImplementedError:
            py = str
        if py is datetime:
            return datetime.fromtimestamp(ms / 1000, tz=UTC).replace(tzinfo=None)
        return ms

    def scan_keys(self, name: str) -> tuple[dict[str, Any], list[str]]:
        """Return ``{record_key: pk}`` for every row, plus duplicate-key warnings."""
        obj, t = self._obj(name)
        pk, mcol = t.c[obj.spec.primary_key], t.c[obj.spec.match.warehouse]
        keys: dict[str, Any] = {}
        dupes: list[str] = []
        with self.engine.connect() as conn:
            for r in conn.execute(select(pk, mcol).order_by(pk)):
                key = obj.normalize_key(r[1])
                if key is None:
                    continue
                if key in keys:
                    dupes.append(f"{name}: duplicate match key {key!r} (pk {r[0]!r} ignored)")
                    continue
                keys[key] = r[0]
        return keys, dupes

    def scan_refs(self, name: str) -> dict[str, dict[str, Any]]:
        """``{record_key: {via_column: value}}`` for every row: a cheap projection used to diff
        associations as sets (so a late-arriving target heals old references)."""
        obj, t = self._obj(name)
        vias = sorted({a.via for a in obj.spec.associations})
        if not vias:
            return {}
        out: dict[str, dict[str, Any]] = {}
        cols = [t.c[obj.spec.match.warehouse], *[t.c[v] for v in vias]]
        with self.engine.connect() as conn:
            for r in conn.execute(select(*cols).order_by(t.c[obj.spec.primary_key])):
                key = obj.normalize_key(r[0])
                if key is not None and key not in out:
                    out[key] = dict(zip(vias, r[1:], strict=True))
        return out

    def fetch_changed(self, name: str, since_ms: int | None) -> list[Row]:
        obj, t = self._obj(name)
        cols = [t.c[c] for c in sorted({*obj.source_columns, obj.spec.updated_at})]
        q = select(*cols)
        if since_ms is not None:
            ucol = obj.spec.updated_at
            q = q.where(t.c[ucol] >= self._ms_to_param(t, ucol, since_ms))
        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(q).mappings()]

    def fetch_by_pks(self, name: str, pks: Iterable[Any]) -> list[Row]:
        obj, t = self._obj(name)
        cols = [t.c[c] for c in sorted({*obj.source_columns, obj.spec.updated_at})]
        out: list[Row] = []
        pk_list = list(pks)
        with self.engine.connect() as conn:
            for chunk in chunked(pk_list, 500):
                q = select(*cols).where(t.c[obj.spec.primary_key].in_(chunk))
                out.extend(dict(r) for r in conn.execute(q).mappings())
        return out

    def row_hash(self, name: str, row: Mapping[str, Any]) -> str:
        obj, _ = self._obj(name)
        payload = [[c, canon(row.get(c))] for c in obj.source_columns]
        return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()

    def row_ts_ms(self, name: str, row: Mapping[str, Any]) -> int | None:
        obj, _ = self._obj(name)
        return _ts_to_ms(row.get(obj.spec.updated_at))

    def to_column_value(self, name: str, column: str, value: CanonValue) -> Any:
        """Convert a canonical string back to the column's Python type for write-back."""
        _, t = self._obj(name)
        if value is None:
            return None
        try:
            py = t.c[column].type.python_type
        except NotImplementedError:
            return value
        if py is int:
            return int(Decimal(value))
        if py is float:
            return float(value)
        if py is Decimal:
            return Decimal(value)
        if py is datetime:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        if py is date:
            return date.fromisoformat(value[:10])
        if py is bool:
            return value == "true"
        return value

    def writeback(self, name: str, pk: Any, changes: Mapping[str, tuple[Any, Any]]) -> bool:
        """Compare-and-swap update: ``changes = {column: (expected_raw, new_value)}``.

        Returns False (and writes nothing) if any column no longer holds the expected value,
        i.e. somebody edited the row after we read it.
        """
        obj, t = self._obj(name)
        conds: list[ColumnElement[bool]] = [t.c[obj.spec.primary_key] == pk]
        for col, (expected, _new) in changes.items():
            c: Column[Any] = t.c[col]
            conds.append(c.is_(None) if expected is None else c == expected)
        stmt = update(t).where(and_(*conds)).values({c: new for c, (_e, new) in changes.items()})
        with self.engine.begin() as conn:
            return conn.execute(stmt).rowcount == 1

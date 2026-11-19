"""Durable sync state: per-record base snapshots, watermarks, dead letters and association base.

The base snapshot keeps the last value *seen on each side* per field (``(base_wh, base_crm)``),
which is what makes field-level three-way merges possible even for fields where the two sides are
allowed to differ by policy (e.g. ``crm_wins``).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar

from sqlalchemy import (
    BigInteger,
    Column,
    Engine,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    delete,
    insert,
    select,
    update,
)

from crm_sync.values import CanonValue

T = TypeVar("T")

metadata = MetaData()

sync_base = Table(
    "sync_base",
    metadata,
    Column("object", String(64), primary_key=True),
    Column("record_key", String(512), primary_key=True),
    Column("wh_pk", String(256)),
    Column("crm_id", String(64)),
    Column("fields", Text, nullable=False),
    Column("wh_hash", String(64)),
    Column("crm_ts", BigInteger),
    Column("tombstone", String(16)),
    Column("updated_ms", BigInteger),
)

watermarks = Table(
    "sync_watermarks",
    metadata,
    Column("object", String(64), primary_key=True),
    Column("side", String(16), primary_key=True),
    Column("value_ms", BigInteger, nullable=False),
)

dead_letters = Table(
    "sync_dead_letters",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("object", String(64), nullable=False),
    Column("record_key", String(512), nullable=False),
    Column("op", String(32), nullable=False),
    Column("op_id", String(64)),
    Column("error", Text, nullable=False),
    Column("payload", Text),
    Column("attempts", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("created_ms", BigInteger, nullable=False),
    Column("updated_ms", BigInteger, nullable=False),
)

assoc_base = Table(
    "sync_assoc_base",
    metadata,
    Column("from_object", String(64), primary_key=True),
    Column("from_key", String(512), primary_key=True),
    Column("to_object", String(64), primary_key=True),
    Column("to_key", String(512), primary_key=True),
)

Tombstone = Literal["wh_deleted", "crm_deleted"]


@dataclass
class BaseRecord:
    key: str
    wh_pk: str | None
    crm_id: str | None
    fields: dict[str, tuple[CanonValue, CanonValue]]
    wh_hash: str | None
    crm_ts: int | None
    tombstone: str | None = None


@dataclass
class DeadLetter:
    id: int
    object: str
    record_key: str
    op: str
    op_id: str | None
    error: str
    attempts: int
    status: str
    payload: dict[str, Any] = field(default_factory=dict)


def chunked(items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class StateStore:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        metadata.create_all(engine)

    # -- base snapshots ---------------------------------------------------------------------
    def load_base(self, obj: str) -> dict[str, BaseRecord]:
        with self.engine.connect() as conn:
            rows = conn.execute(select(sync_base).where(sync_base.c.object == obj)).mappings()
            out: dict[str, BaseRecord] = {}
            for r in rows:
                raw = json.loads(r["fields"])
                out[r["record_key"]] = BaseRecord(
                    key=r["record_key"],
                    wh_pk=r["wh_pk"],
                    crm_id=r["crm_id"],
                    fields={k: (v[0], v[1]) for k, v in raw.items()},
                    wh_hash=r["wh_hash"],
                    crm_ts=r["crm_ts"],
                    tombstone=r["tombstone"],
                )
            return out

    def save_base(self, obj: str, records: Iterable[BaseRecord], now_ms: int) -> None:
        recs = list(records)
        if not recs:
            return
        with self.engine.begin() as conn:
            for chunk in chunked(recs, 400):
                keys = [r.key for r in chunk]
                conn.execute(
                    delete(sync_base).where(
                        and_(sync_base.c.object == obj, sync_base.c.record_key.in_(keys))
                    )
                )
                conn.execute(
                    insert(sync_base),
                    [
                        {
                            "object": obj,
                            "record_key": r.key,
                            "wh_pk": r.wh_pk,
                            "crm_id": r.crm_id,
                            "fields": json.dumps(
                                {k: [v[0], v[1]] for k, v in sorted(r.fields.items())}
                            ),
                            "wh_hash": r.wh_hash,
                            "crm_ts": r.crm_ts,
                            "tombstone": r.tombstone,
                            "updated_ms": now_ms,
                        }
                        for r in chunk
                    ],
                )

    # -- watermarks -------------------------------------------------------------------------
    def get_watermark(self, obj: str, side: str) -> int | None:
        with self.engine.connect() as conn:
            v = conn.execute(
                select(watermarks.c.value_ms).where(
                    and_(watermarks.c.object == obj, watermarks.c.side == side)
                )
            ).scalar()
            return int(v) if v is not None else None

    def set_watermarks(self, values: dict[tuple[str, str], int]) -> None:
        with self.engine.begin() as conn:
            for (obj, side), value in values.items():
                conn.execute(
                    delete(watermarks).where(
                        and_(watermarks.c.object == obj, watermarks.c.side == side)
                    )
                )
                conn.execute(insert(watermarks).values(object=obj, side=side, value_ms=value))

    # -- dead letters -----------------------------------------------------------------------
    def dlq_add(
        self,
        obj: str,
        key: str,
        op: str,
        error: str,
        now_ms: int,
        op_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Record a failure. One open entry per (object, key); repeats bump ``attempts``."""
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(dead_letters.c.id, dead_letters.c.attempts).where(
                    and_(
                        dead_letters.c.object == obj,
                        dead_letters.c.record_key == key,
                        dead_letters.c.status == "open",
                    )
                )
            ).first()
            body = json.dumps(payload or {}, default=str)
            if existing is not None:
                conn.execute(
                    update(dead_letters)
                    .where(dead_letters.c.id == existing.id)
                    .values(
                        attempts=existing.attempts + 1,
                        error=error,
                        op=op,
                        op_id=op_id,
                        payload=body,
                        updated_ms=now_ms,
                    )
                )
            else:
                conn.execute(
                    insert(dead_letters).values(
                        object=obj,
                        record_key=key,
                        op=op,
                        op_id=op_id,
                        error=error,
                        payload=body,
                        attempts=1,
                        status="open",
                        created_ms=now_ms,
                        updated_ms=now_ms,
                    )
                )

    def dlq_list(self, obj: str | None = None, status: str = "open") -> list[DeadLetter]:
        q = select(dead_letters).where(dead_letters.c.status == status)
        if obj is not None:
            q = q.where(dead_letters.c.object == obj)
        with self.engine.connect() as conn:
            return [
                DeadLetter(
                    id=r["id"],
                    object=r["object"],
                    record_key=r["record_key"],
                    op=r["op"],
                    op_id=r["op_id"],
                    error=r["error"],
                    attempts=r["attempts"],
                    status=r["status"],
                    payload=json.loads(r["payload"] or "{}"),
                )
                for r in conn.execute(q.order_by(dead_letters.c.id)).mappings()
            ]

    def dlq_resolve(self, obj: str, keys: Iterable[str], now_ms: int) -> int:
        ks = list(keys)
        if not ks:
            return 0
        n = 0
        with self.engine.begin() as conn:
            for chunk in chunked(ks, 400):
                res = conn.execute(
                    update(dead_letters)
                    .where(
                        and_(
                            dead_letters.c.object == obj,
                            dead_letters.c.record_key.in_(chunk),
                            dead_letters.c.status == "open",
                        )
                    )
                    .values(status="resolved", updated_ms=now_ms)
                )
                n += res.rowcount
        return n

    # -- associations -----------------------------------------------------------------------
    def assoc_load(self, from_obj: str, to_obj: str) -> dict[str, set[str]]:
        out: dict[str, set[str]] = {}
        with self.engine.connect() as conn:
            for r in conn.execute(
                select(assoc_base.c.from_key, assoc_base.c.to_key).where(
                    and_(assoc_base.c.from_object == from_obj, assoc_base.c.to_object == to_obj)
                )
            ):
                out.setdefault(r.from_key, set()).add(r.to_key)
        return out

    def assoc_apply(
        self,
        from_obj: str,
        to_obj: str,
        add: Iterable[tuple[str, str]],
        remove: Iterable[tuple[str, str]],
    ) -> None:
        adds, removes = list(add), list(remove)
        with self.engine.begin() as conn:
            for fk, tk in removes:
                conn.execute(
                    delete(assoc_base).where(
                        and_(
                            assoc_base.c.from_object == from_obj,
                            assoc_base.c.to_object == to_obj,
                            assoc_base.c.from_key == fk,
                            assoc_base.c.to_key == tk,
                        )
                    )
                )
            if adds:
                existing = {
                    (r.from_key, r.to_key)
                    for r in conn.execute(
                        select(assoc_base.c.from_key, assoc_base.c.to_key).where(
                            and_(
                                assoc_base.c.from_object == from_obj,
                                assoc_base.c.to_object == to_obj,
                            )
                        )
                    )
                }
                fresh = [p for p in dict.fromkeys(adds) if p not in existing]
                if fresh:
                    conn.execute(
                        insert(assoc_base),
                        [
                            {
                                "from_object": from_obj,
                                "from_key": f,
                                "to_object": to_obj,
                                "to_key": t,
                            }
                            for f, t in fresh
                        ],
                    )

    def assoc_forget(self, from_obj: str, keys: Iterable[str]) -> None:
        ks = list(keys)
        with self.engine.begin() as conn:
            for chunk in chunked(ks, 400):
                conn.execute(
                    delete(assoc_base).where(
                        and_(assoc_base.c.from_object == from_obj, assoc_base.c.from_key.in_(chunk))
                    )
                )

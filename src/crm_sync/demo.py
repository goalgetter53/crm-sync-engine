"""Synthetic warehouse used by the quickstart, tests and the benchmark (deterministic by seed)."""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Engine,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    insert,
)

metadata = MetaData()

companies = Table(
    "companies",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(200)),
    Column("domain", String(200)),
    Column("industry", String(100)),
    Column("employees", Integer),
    Column("updated_at", DateTime),
)

contacts = Table(
    "contacts",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("email", String(320)),
    Column("first_name", String(100)),
    Column("last_name", String(100)),
    Column("phone", String(50)),
    Column("lifecycle_stage", String(50)),
    Column("job_title", String(200)),
    Column("company_id", Integer),
    Column("updated_at", DateTime),
)

deals = Table(
    "deals",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("name", String(200)),
    Column("amount", Numeric(14, 2)),
    Column("stage", String(50)),
    Column("close_date", Date),
    Column("company_id", Integer),
    Column("contact_email", String(320)),
    Column("updated_at", DateTime),
)

FIRST = ["Ava", "Liam", "Noah", "Emma", "Mia", "Zara", "Omar", "Priya", "Chen", "Sofia", "Ivan"]
LAST = ["Patel", "Smith", "Garcia", "Khan", "Nguyen", "Okafor", "Rossi", "Kim", "Silva", "Mehta"]
WORDS = ["Acme", "Globex", "Initech", "Umbrella", "Stark", "Wayne", "Hooli", "Vandelay", "Wonka"]
SUFFIX = ["Labs", "Health", "Systems", "Analytics", "Devices", "Logistics", "Cloud"]
INDUSTRY = ["COMPUTER_SOFTWARE", "HOSPITAL_HEALTH_CARE", "MEDICAL_DEVICES", "LOGISTICS", None]
STAGES = ["appointmentscheduled", "qualifiedtobuy", "contractsent", "closedwon", "closedlost"]
LIFECYCLE = ["lead", "mql", "sql", "opportunity", "customer"]


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _phone(rng: random.Random) -> str | None:
    n = f"{rng.randint(200, 999)}{rng.randint(200, 999)}{rng.randint(1000, 9999)}"
    style = rng.randint(0, 4)
    if style == 0:
        return None
    if style == 1:
        return f"({n[:3]}) {n[3:6]}-{n[6:]}"
    if style == 2:
        return f"{n[:3]}.{n[3:6]}.{n[6:]}"
    if style == 3:
        return f"+1 {n[:3]} {n[3:6]} {n[6:]}"
    return n


def generate(
    n_companies: int,
    n_contacts: int,
    n_deals: int,
    *,
    seed: int = 7,
    bad_rows: int = 0,
    now: datetime | None = None,
) -> dict[str, list[dict[str, Any]]]:
    rng = random.Random(seed)
    ts = now or utcnow()
    comp = []
    for i in range(1, n_companies + 1):
        w, s = rng.choice(WORDS), rng.choice(SUFFIX)
        comp.append(
            {
                "id": i,
                "name": f"{w} {s} {i}",
                "domain": f"{w.lower()}{i}.example.com",
                "industry": rng.choice(INDUSTRY),
                "employees": rng.randint(5, 5000),
                "updated_at": ts,
            }
        )
    cont = []
    for i in range(1, n_contacts + 1):
        f, la = rng.choice(FIRST), rng.choice(LAST)
        cid = rng.randint(1, n_companies) if n_companies else None
        email = f"{f}.{la}{i}@mail{i % 97}.example.com"
        if i % 7 == 0:
            email = f"  {email.upper()} "  # messy source data; normalized by the mapping
        cont.append(
            {
                "id": i,
                "email": email,
                "first_name": f,
                "last_name": la,
                "phone": _phone(rng),
                "lifecycle_stage": "zombie" if i <= bad_rows else rng.choice(LIFECYCLE),
                "job_title": rng.choice([None, "VP Sales", "Engineer", "CFO", "Buyer"]),
                "company_id": cid,
                "updated_at": ts,
            }
        )
    dl = []
    for i in range(1, n_deals + 1):
        contact = cont[rng.randrange(len(cont))] if cont else None
        dl.append(
            {
                "id": 10_000_000 + i,
                "name": f"Deal {i}",
                "amount": Decimal(rng.randint(500, 250_000)) + Decimal(rng.choice([0, 50])) / 100,
                "stage": rng.choice(STAGES),
                "close_date": date(2026, 1, 1) + timedelta(days=rng.randint(0, 364)),
                "company_id": contact["company_id"] if contact else None,
                "contact_email": str(contact["email"]).strip().lower() if contact else None,
                "updated_at": ts,
            }
        )
    return {"companies": comp, "contacts": cont, "deals": dl}


def seed(
    engine: Engine,
    *,
    companies_n: int = 50,
    contacts_n: int = 500,
    deals_n: int = 100,
    seed_value: int = 7,
    bad_rows: int = 0,
    now: datetime | None = None,
) -> dict[str, int]:
    metadata.drop_all(engine)
    metadata.create_all(engine)
    data = generate(companies_n, contacts_n, deals_n, seed=seed_value, bad_rows=bad_rows, now=now)
    with engine.begin() as conn:
        for name, table in (("companies", companies), ("contacts", contacts), ("deals", deals)):
            rows = data[name]
            for i in range(0, len(rows), 5000):
                if rows[i : i + 5000]:
                    conn.execute(insert(table), rows[i : i + 5000])
    return {k: len(v) for k, v in data.items()}

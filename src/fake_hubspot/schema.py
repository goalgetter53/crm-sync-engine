"""Property schemas for the fake portal (a realistic subset of HubSpot's defaults)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PropertyDef:
    name: str
    type: str = "string"  # string | number | enumeration | date
    options: tuple[str, ...] = ()
    unique: bool = False
    readonly: bool = False
    email: bool = False


LIFECYCLE = (
    "subscriber",
    "lead",
    "marketingqualifiedlead",
    "salesqualifiedlead",
    "opportunity",
    "customer",
    "evangelist",
    "other",
)
DEAL_STAGES = (
    "appointmentscheduled",
    "qualifiedtobuy",
    "presentationscheduled",
    "decisionmakerboughtin",
    "contractsent",
    "closedwon",
    "closedlost",
)

_SYSTEM = (
    PropertyDef("hs_object_id", readonly=True),
    PropertyDef("hs_lastmodifieddate", readonly=True),
    PropertyDef("createdate", readonly=True),
)

SCHEMAS: dict[str, dict[str, PropertyDef]] = {
    "contacts": {
        p.name: p
        for p in (
            *_SYSTEM,
            PropertyDef("email", unique=True, email=True),
            PropertyDef("firstname"),
            PropertyDef("lastname"),
            PropertyDef("phone"),
            PropertyDef("company"),
            PropertyDef("jobtitle"),
            PropertyDef("lifecyclestage", "enumeration", LIFECYCLE),
            PropertyDef("warehouse_id", unique=True),
        )
    },
    "companies": {
        p.name: p
        for p in (
            *_SYSTEM,
            PropertyDef("name"),
            PropertyDef("domain"),
            PropertyDef("industry"),
            PropertyDef("numberofemployees", "number"),
            PropertyDef("annualrevenue", "number"),
            PropertyDef("city"),
            PropertyDef("country"),
            PropertyDef("lifecyclestage", "enumeration", LIFECYCLE),
            PropertyDef("warehouse_id", unique=True),
        )
    },
    "deals": {
        p.name: p
        for p in (
            *_SYSTEM,
            PropertyDef("dealname"),
            PropertyDef("amount", "number"),
            PropertyDef("dealstage", "enumeration", DEAL_STAGES),
            PropertyDef("pipeline", "enumeration", ("default",)),
            PropertyDef("closedate", "date"),
            PropertyDef("warehouse_id", unique=True),
        )
    },
}

OBJECT_TYPE_IDS = {"contacts": "0-1", "companies": "0-2", "deals": "0-3"}

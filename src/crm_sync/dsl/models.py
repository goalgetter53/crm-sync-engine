"""Pydantic models for the YAML mapping DSL (structure only; semantics live in loader.py)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Policy = Literal[
    "source_wins", "crm_wins", "latest_timestamp_wins", "fill_if_empty", "never_overwrite"
]
Direction = Literal["to_crm", "bidirectional", "to_warehouse"]
CrmObjectType = Literal["contacts", "companies", "deals"]

POLICIES: tuple[str, ...] = (
    "source_wins",
    "crm_wins",
    "latest_timestamp_wins",
    "fill_if_empty",
    "never_overwrite",
)
DIRECTIONS: tuple[str, ...] = ("to_crm", "bidirectional", "to_warehouse")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FieldMapping(_Strict):
    crm: str = Field(min_length=1, description="HubSpot property name")
    warehouse: str | None = Field(None, description="Warehouse column (omit for computed)")
    transform: str | None = Field(None, description="Expression; `value` is the column value")
    direction: Direction = "to_crm"
    policy: Policy = "source_wins"


class MatchRule(_Strict):
    warehouse: str
    crm: str
    normalize: str | None = None


class AssociationMapping(_Strict):
    to: str = Field(description="Target object name in this mapping")
    via: str = Field(description="Warehouse column holding the target's match key")


class ObjectMapping(_Strict):
    crm_object: CrmObjectType
    table: str
    primary_key: str = "id"
    updated_at: str = "updated_at"
    match: MatchRule
    fields: list[FieldMapping] = Field(min_length=1)
    associations: list[AssociationMapping] = Field(default_factory=list)
    archive_on_delete: bool = True


class Settings(_Strict):
    batch_size: int = Field(100, ge=1, le=100)
    search_page_size: int = Field(200, ge=1, le=200)
    search_lag_seconds: float = Field(30.0, ge=0)
    warehouse_lookback_seconds: float = Field(300.0, ge=0)
    requests_per_10s: int = Field(100, ge=1)
    search_requests_per_second: float = Field(4.0, gt=0)
    daily_request_limit: int = Field(250_000, ge=1)
    max_retries: int = Field(6, ge=0)
    revalidate_before_write: bool = True
    max_auto_retries: int = Field(3, ge=0)


class MappingConfig(_Strict):
    version: Literal[1]
    settings: Settings = Field(default_factory=lambda: Settings())
    maps: dict[str, dict[str, str]] = Field(default_factory=dict)
    objects: dict[str, ObjectMapping] = Field(min_length=1)

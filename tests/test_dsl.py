from __future__ import annotations

import copy
from typing import Any

import pytest
import yaml

from crm_sync.dsl import MappingError, compile_mapping, load_mapping
from tests.conftest import EXAMPLE_MAPPING


def base() -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load(EXAMPLE_MAPPING.read_text())
    return copy.deepcopy(data)


def errors_for(data: dict[str, Any]) -> list[str]:
    with pytest.raises(MappingError) as exc:
        compile_mapping(data)
    return exc.value.errors


def test_example_mapping_compiles_in_dependency_order() -> None:
    m = load_mapping(EXAMPLE_MAPPING)
    assert m.order == ("companies", "contacts", "deals")
    contacts = m.objects["contacts"]
    assert contacts.key_from_row({"email": "  A@B.com "}) == "a@b.com"
    assert "company_id" in contacts.source_columns


def test_unknown_policy_has_precise_path() -> None:
    d = base()
    d["objects"]["contacts"]["fields"][1]["policy"] = "warehouse_rules"
    errs = errors_for(d)
    assert any(e.startswith("objects.contacts.fields[1].policy") for e in errs)


def test_unknown_keys_are_rejected() -> None:
    d = base()
    d["objects"]["contacts"]["feilds"] = []
    assert any("feilds" in e for e in errors_for(d))


def test_batch_size_capped_at_hubspot_limit() -> None:
    d = base()
    d["settings"]["batch_size"] = 500
    assert any("settings.batch_size" in e for e in errors_for(d))


def test_bidirectional_requires_idempotent_transform() -> None:
    d = base()
    d["objects"]["contacts"]["fields"][3]["transform"] = "concat(value, ' ext')"
    errs = errors_for(d)
    assert any("cannot be safely round-tripped" in e for e in errs)


def test_computed_fields_cannot_write_back() -> None:
    d = base()
    d["objects"]["deals"]["fields"][4]["direction"] = "bidirectional"
    assert any("nowhere to write back" in e for e in errors_for(d))


def test_to_warehouse_requires_crm_wins() -> None:
    d = base()
    d["objects"]["contacts"]["fields"][5].update(direction="to_warehouse", policy="source_wins")
    assert any("requires policy crm_wins" in e for e in errors_for(d))


def test_duplicate_property_and_read_only_property() -> None:
    d = base()
    fields = d["objects"]["contacts"]["fields"]
    fields.append({"crm": "firstname", "warehouse": "first_name"})
    fields.append({"crm": "hs_lead_score", "warehouse": "first_name"})
    errs = errors_for(d)
    assert any("mapped more than once" in e for e in errs)
    assert any("read-only HubSpot system property" in e for e in errs)


def test_association_target_typo_gets_suggestion() -> None:
    d = base()
    d["objects"]["contacts"]["associations"][0]["to"] = "compnies"
    assert any("did you mean 'companies'" in e for e in errors_for(d))


def test_association_cycle_detected() -> None:
    d = base()
    d["objects"]["companies"]["associations"] = [{"to": "deals", "via": "id"}]
    assert any("association cycle" in e for e in errors_for(d))


def test_match_property_must_be_owned_by_warehouse() -> None:
    d = base()
    d["objects"]["contacts"]["fields"][0]["policy"] = "crm_wins"
    assert any("match property 'email' must use source_wins" in e for e in errors_for(d))


def test_all_errors_reported_at_once() -> None:
    d = base()
    d["objects"]["contacts"]["fields"][1]["transform"] = "upper(value"
    d["objects"]["deals"]["fields"][1]["transform"] = "nope(value)"
    d["objects"]["deals"]["crm_object"] = "contacts"
    errs = errors_for(d)
    assert len(errs) >= 3
    assert "3 error(s)" in str(MappingError(errs[:3]))


def test_yaml_syntax_error_is_a_mapping_error() -> None:
    with pytest.raises(MappingError, match="yaml"):
        load_mapping("version: 1\nobjects: [unclosed\n")

"""Load, validate and compile a mapping file into an executable :class:`CompiledMapping`."""

from __future__ import annotations

import difflib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from crm_sync.dsl.expr import ExprError, Expression, compile_expr
from crm_sync.dsl.functions import Maps
from crm_sync.dsl.models import FieldMapping, MappingConfig, ObjectMapping, Settings
from crm_sync.values import CanonValue, canon


class MappingError(ValueError):
    """Raised with every problem found, each prefixed with a path into the YAML document."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        bullet = "\n".join(f"  - {e}" for e in errors)
        super().__init__(f"invalid mapping ({len(errors)} error(s)):\n{bullet}")


@dataclass(frozen=True)
class CompiledField:
    spec: FieldMapping
    expr: Expression | None

    @property
    def name(self) -> str:
        return self.spec.crm

    def from_warehouse(self, row: Mapping[str, Any], maps: Maps) -> CanonValue:
        raw = row.get(self.spec.warehouse) if self.spec.warehouse else None
        return canon(self.expr(raw, row, maps) if self.expr else raw)

    def from_crm(self, raw: Any, maps: Maps) -> CanonValue:
        # Fields that write back are normalized identically on both sides (normalizer chains are
        # idempotent), so e.g. "Alice@X.com" in the CRM equals "alice@x.com" in the warehouse.
        if self.expr is not None and self.spec.direction != "to_crm":
            return canon(self.expr(raw, {}, maps))
        return canon(raw)


@dataclass(frozen=True)
class CompiledObject:
    name: str
    spec: ObjectMapping
    fields: tuple[CompiledField, ...]
    match_normalize: Expression | None
    maps: Maps

    @property
    def crm_object(self) -> str:
        return self.spec.crm_object

    @property
    def crm_properties(self) -> list[str]:
        props = [f.name for f in self.fields]
        if self.spec.match.crm not in props:
            props.append(self.spec.match.crm)
        return props

    @property
    def source_columns(self) -> tuple[str, ...]:
        """Columns that influence what we send; their hash is the change-detection snapshot."""
        cols = {self.spec.primary_key, self.spec.match.warehouse}
        for f in self.fields:
            if f.spec.warehouse:
                cols.add(f.spec.warehouse)
            if f.expr:
                cols |= f.expr.columns
        cols |= {a.via for a in self.spec.associations}
        return tuple(sorted(cols))

    def normalize_key(self, raw: Any) -> str | None:
        v = self.match_normalize(raw, {}, self.maps) if self.match_normalize else raw
        return canon(v)

    def key_from_row(self, row: Mapping[str, Any]) -> str | None:
        return self.normalize_key(row.get(self.spec.match.warehouse))

    def key_from_crm(self, props: Mapping[str, Any]) -> str | None:
        return self.normalize_key(props.get(self.spec.match.crm))

    def warehouse_values(self, row: Mapping[str, Any]) -> dict[str, CanonValue]:
        return {f.name: f.from_warehouse(row, self.maps) for f in self.fields}

    def crm_values(self, props: Mapping[str, Any]) -> dict[str, CanonValue]:
        return {f.name: f.from_crm(props.get(f.name), self.maps) for f in self.fields}

    def field(self, name: str) -> CompiledField:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(name)


@dataclass(frozen=True)
class CompiledMapping:
    config: MappingConfig
    objects: dict[str, CompiledObject]
    order: tuple[str, ...]  # association targets before sources
    warnings: list[str] = field(default_factory=list)

    @property
    def settings(self) -> Settings:
        return self.config.settings


def _fmt_pydantic(err: ValidationError) -> list[str]:
    out = []
    for e in err.errors():
        loc = ".".join(str(p) if not isinstance(p, int) else f"[{p}]" for p in e["loc"])
        out.append(f"{loc.replace('.[', '[')}: {e['msg']}")
    return out


def _suggest(name: str, options: list[str]) -> str:
    close = difflib.get_close_matches(name, options, n=1)
    return f" (did you mean {close[0]!r}?)" if close else ""


def _toposort(cfg: MappingConfig, errors: list[str]) -> tuple[str, ...]:
    deps = {
        n: {a.to for a in o.associations if a.to in cfg.objects} for n, o in cfg.objects.items()
    }
    order: list[str] = []
    state: dict[str, int] = {}

    def visit(n: str, stack: list[str]) -> None:
        if state.get(n) == 2:
            return
        if state.get(n) == 1:
            errors.append(f"objects: association cycle {' -> '.join([*stack, n])}")
            return
        state[n] = 1
        for d in sorted(deps[n]):
            if d != n:
                visit(d, [*stack, n])
        state[n] = 2
        order.append(n)

    for n in cfg.objects:
        visit(n, [])
    return tuple(order)


def compile_mapping(data: Mapping[str, Any]) -> CompiledMapping:
    try:
        cfg = MappingConfig.model_validate(data)
    except ValidationError as exc:
        raise MappingError(_fmt_pydantic(exc)) from None

    errors: list[str] = []
    warnings: list[str] = []
    objects: dict[str, CompiledObject] = {}
    seen_crm_types: dict[str, str] = {}

    for oname, obj in cfg.objects.items():
        base = f"objects.{oname}"
        if obj.crm_object in seen_crm_types:
            errors.append(
                f"{base}.crm_object: {obj.crm_object!r} already mapped by "
                f"{seen_crm_types[obj.crm_object]!r}; each CRM object type may be mapped once"
            )
        seen_crm_types[obj.crm_object] = oname

        match_norm: Expression | None = None
        if obj.match.normalize:
            try:
                match_norm = compile_expr(obj.match.normalize, cfg.maps)
                if not match_norm.is_normalizer_chain:
                    errors.append(
                        f"{base}.match.normalize: must be a normalizer chain over `value` "
                        "(e.g. lower(trim(value)))"
                    )
            except ExprError as exc:
                errors.append(f"{base}.match.normalize: {exc}")

        compiled_fields: list[CompiledField] = []
        seen_props: set[str] = set()
        for i, f in enumerate(obj.fields):
            path = f"{base}.fields[{i}]"
            if f.crm in seen_props:
                errors.append(f"{path}.crm: property {f.crm!r} mapped more than once")
            seen_props.add(f.crm)
            if f.crm.startswith("hs_") or f.crm in {"createdate", "lastmodifieddate"}:
                errors.append(f"{path}.crm: {f.crm!r} is a read-only HubSpot system property")
            if f.warehouse is None and f.transform is None:
                errors.append(f"{path}: needs `warehouse` column, `transform`, or both")
            expr: Expression | None = None
            if f.transform is not None:
                try:
                    expr = compile_expr(f.transform, cfg.maps)
                except ExprError as exc:
                    errors.append(f"{path}.transform: {exc}")
            if f.direction != "to_crm":
                if f.warehouse is None:
                    errors.append(
                        f"{path}: computed field cannot use direction {f.direction!r} "
                        "(nowhere to write back)"
                    )
                if expr is not None and not expr.is_normalizer_chain:
                    errors.append(
                        f"{path}.transform: direction {f.direction!r} requires an idempotent "
                        "normalizer chain over `value` (lower/upper/trim/title/e164/to_str); "
                        f"{f.transform!r} cannot be safely round-tripped"
                    )
            if f.direction == "to_warehouse" and f.policy != "crm_wins":
                errors.append(f"{path}.policy: direction 'to_warehouse' requires policy crm_wins")
            if f.crm == obj.match.crm and f.direction == "to_warehouse":
                errors.append(f"{path}: the match property cannot be owned by the CRM")
            if f.crm == obj.match.crm and f.policy != "source_wins":
                errors.append(
                    f"{path}.policy: match property {f.crm!r} must use source_wins "
                    "(the record key is owned by the warehouse)"
                )
            if expr is not None and f.warehouse is not None and not expr.uses_value:
                warnings.append(f"{path}: transform ignores `value`; column {f.warehouse!r} unused")
            compiled_fields.append(CompiledField(spec=f, expr=expr))

        for j, a in enumerate(obj.associations):
            if a.to not in cfg.objects:
                errors.append(
                    f"{base}.associations[{j}].to: unknown object {a.to!r}"
                    f"{_suggest(a.to, list(cfg.objects))}"
                )
            elif a.to == oname:
                errors.append(f"{base}.associations[{j}].to: self-associations are not supported")

        objects[oname] = CompiledObject(
            name=oname,
            spec=obj,
            fields=tuple(compiled_fields),
            match_normalize=match_norm,
            maps=cfg.maps,
        )

    order = _toposort(cfg, errors)
    if errors:
        raise MappingError(errors)
    return CompiledMapping(config=cfg, objects=objects, order=order, warnings=warnings)


def load_mapping(source: str | Path) -> CompiledMapping:
    """Load from a path, or from YAML text when ``source`` contains a newline."""
    if isinstance(source, Path) or "\n" not in source:
        text = Path(source).read_text(encoding="utf-8")
    else:
        text = source
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise MappingError([f"yaml: {exc}"]) from None
    if not isinstance(data, dict):
        raise MappingError(["document root must be a mapping"])
    return compile_mapping(data)

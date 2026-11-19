"""Declarative mapping DSL."""

from crm_sync.dsl.loader import (
    CompiledField,
    CompiledMapping,
    CompiledObject,
    MappingError,
    compile_mapping,
    load_mapping,
)

__all__ = [
    "CompiledField",
    "CompiledMapping",
    "CompiledObject",
    "MappingError",
    "compile_mapping",
    "load_mapping",
]

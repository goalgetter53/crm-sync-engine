"""A tiny, safe expression language for field transforms.

Grammar::

    expr    := call | literal | ref
    call    := IDENT "(" [expr ("," expr)*] ")"
    ref     := "value" | "row" "." IDENT
    literal := STRING | NUMBER | "null" | "true" | "false"

There is no attribute access, no operators and no ``eval``: an expression can only call
functions from :mod:`crm_sync.dsl.functions`, so a mapping file cannot execute code.
"""

from __future__ import annotations

import difflib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from crm_sync.dsl.functions import REGISTRY, Maps

MAX_SOURCE_LEN = 500
MAX_DEPTH = 16


class ExprError(ValueError):
    def __init__(self, message: str, pos: int | None = None) -> None:
        super().__init__(message if pos is None else f"{message} (at char {pos})")
        self.pos = pos


@dataclass(frozen=True)
class Lit:
    value: Any


@dataclass(frozen=True)
class ValueRef:
    pass


@dataclass(frozen=True)
class RowRef:
    column: str


@dataclass(frozen=True)
class Call:
    name: str
    args: tuple[Node, ...]


Node = Lit | ValueRef | RowRef | Call

_TOKEN = re.compile(
    r"""\s*(?:
        (?P<string>"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')
      | (?P<number>-?\d+(?:\.\d+)?)
      | (?P<ident>[A-Za-z_][A-Za-z0-9_]*)
      | (?P<punct>[(),.])
    )""",
    re.VERBOSE,
)


def _tokenize(src: str) -> list[tuple[str, str, int]]:
    tokens: list[tuple[str, str, int]] = []
    pos = 0
    while pos < len(src):
        if src[pos:].strip() == "":
            break
        m = _TOKEN.match(src, pos)
        if not m or m.end() == pos:
            raise ExprError(f"unexpected character {src[pos:].lstrip()[:1]!r}", pos)
        kind = m.lastgroup
        assert kind is not None
        tokens.append((kind, m.group(kind), m.start(kind)))
        pos = m.end()
    return tokens


class _Parser:
    def __init__(self, src: str) -> None:
        self.tokens = _tokenize(src)
        self.i = 0

    def _peek(self) -> tuple[str, str, int] | None:
        return self.tokens[self.i] if self.i < len(self.tokens) else None

    def _next(self) -> tuple[str, str, int]:
        tok = self._peek()
        if tok is None:
            raise ExprError("unexpected end of expression")
        self.i += 1
        return tok

    def _expect(self, text: str) -> None:
        _kind, val, pos = self._next()
        if val != text:
            raise ExprError(f"expected {text!r}, got {val!r}", pos)

    def parse(self) -> Node:
        node = self._expr(0)
        tok = self._peek()
        if tok is not None:
            raise ExprError(f"unexpected trailing token {tok[1]!r}", tok[2])
        return node

    def _expr(self, depth: int) -> Node:
        if depth > MAX_DEPTH:
            raise ExprError(f"expression nested deeper than {MAX_DEPTH}")
        kind, val, pos = self._next()
        if kind == "string":
            body = val[1:-1]
            return Lit(re.sub(r"\\(.)", r"\1", body))
        if kind == "number":
            return Lit(Decimal(val) if "." in val else int(val))
        if kind == "ident":
            if val == "null":
                return Lit(None)
            if val in ("true", "false"):
                return Lit(val == "true")
            if val == "value":
                return ValueRef()
            if val == "row":
                self._expect(".")
                k, col, p = self._next()
                if k != "ident":
                    raise ExprError("expected column name after 'row.'", p)
                return RowRef(col)
            nxt = self._peek()
            if nxt is None or nxt[1] != "(":
                raise ExprError(f"unknown identifier {val!r} (use 'value' or 'row.<col>')", pos)
            self._next()
            args: list[Node] = []
            if (p2 := self._peek()) is not None and p2[1] == ")":
                self._next()
            else:
                while True:
                    args.append(self._expr(depth + 1))
                    _k2, v2, pos2 = self._next()
                    if v2 == ")":
                        break
                    if v2 != ",":
                        raise ExprError(f"expected ',' or ')', got {v2!r}", pos2)
            return Call(val, tuple(args))
        raise ExprError(f"unexpected token {val!r}", pos)


def _check(node: Node, maps: Mapping[str, Any]) -> None:
    if not isinstance(node, Call):
        return
    fn = REGISTRY.get(node.name)
    if fn is None:
        close = difflib.get_close_matches(node.name, list(REGISTRY), n=1)
        hint = f" (did you mean {close[0]!r}?)" if close else ""
        raise ExprError(f"unknown function {node.name!r}{hint}")
    n = len(node.args)
    if n < fn.min_args or (fn.max_args is not None and n > fn.max_args):
        want = (
            f"{fn.min_args}"
            if fn.max_args == fn.min_args
            else f"{fn.min_args}..{fn.max_args if fn.max_args is not None else 'n'}"
        )
        raise ExprError(f"{node.name}() takes {want} argument(s), got {n}")
    if fn.needs_maps:
        target = node.args[1]
        if not isinstance(target, Lit) or not isinstance(target.value, str):
            raise ExprError(f"{node.name}() map name must be a string literal")
        if target.value not in maps:
            raise ExprError(f"{node.name}() references undefined map {target.value!r}")
    for a in node.args:
        _check(a, maps)


def _columns(node: Node) -> set[str]:
    if isinstance(node, RowRef):
        return {node.column}
    if isinstance(node, Call):
        out: set[str] = set()
        for a in node.args:
            out |= _columns(a)
        return out
    return set()


def _uses_value(node: Node) -> bool:
    if isinstance(node, ValueRef):
        return True
    if isinstance(node, Call):
        return any(_uses_value(a) for a in node.args)
    return False


def _is_normalizer_chain(node: Node) -> bool:
    """True for ``value`` wrapped only in single-arg normalizers, e.g. ``lower(trim(value))``."""
    if isinstance(node, ValueRef):
        return True
    if isinstance(node, Call):
        fn = REGISTRY[node.name]
        return fn.normalizer and len(node.args) == 1 and _is_normalizer_chain(node.args[0])
    return False


def _eval(node: Node, value: Any, row: Mapping[str, Any], maps: Maps) -> Any:
    if isinstance(node, Lit):
        return node.value
    if isinstance(node, ValueRef):
        return value
    if isinstance(node, RowRef):
        return row.get(node.column)
    fn = REGISTRY[node.name]
    args = [_eval(a, value, row, maps) for a in node.args]
    if fn.needs_maps:
        return fn.impl(*args, maps=maps)
    return fn.impl(*args)


@dataclass(frozen=True)
class Expression:
    source: str
    node: Node
    columns: frozenset[str]
    uses_value: bool
    is_normalizer_chain: bool

    def __call__(self, value: Any, row: Mapping[str, Any], maps: Maps) -> Any:
        return _eval(self.node, value, row, maps)


def compile_expr(source: str, maps: Mapping[str, Any] | None = None) -> Expression:
    if len(source) > MAX_SOURCE_LEN:
        raise ExprError(f"expression longer than {MAX_SOURCE_LEN} characters")
    if not source.strip():
        raise ExprError("empty expression")
    node = _Parser(source).parse()
    _check(node, maps or {})
    return Expression(
        source=source,
        node=node,
        columns=frozenset(_columns(node)),
        uses_value=_uses_value(node),
        is_normalizer_chain=_is_normalizer_chain(node),
    )

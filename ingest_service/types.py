from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ParsedTable:
    header: list[str]
    rows: list[list[str]]


@dataclass(frozen=True)
class ParseDebug:
    type: str
    summary: Any


@dataclass(frozen=True)
class ParseResult:
    table: ParsedTable | None
    debug: ParseDebug

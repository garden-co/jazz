from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


JsonObject = dict[str, Any]


@dataclass(frozen=True)
class OperationResult:
    operation_id: str
    digest_id: str
    status: str
    queued: bool
    record: JsonObject | None = None
    error: str | None = None
    trace_path: Path | None = None


@dataclass(frozen=True)
class ListResult:
    records: list[JsonObject]
    degraded: bool
    error: str | None = None
    trace_path: Path | None = None


@dataclass(frozen=True)
class FlushResult:
    completed: int
    failed: int
    remaining: int

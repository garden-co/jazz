from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


class OperationOutbox:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.pending_dir = root / "pending"
        self.done_dir = root / "done"
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.done_dir.mkdir(parents=True, exist_ok=True)

    def enqueue(self, operation: dict[str, Any], *, error: str | None = None) -> Path:
        operation = dict(operation)
        operation_id = str(operation["operationId"])
        existing = self._read(self._pending_path(operation_id))
        if existing:
            operation["attempts"] = int(existing.get("attempts") or 0)
            operation["createdAt"] = existing.get("createdAt") or operation.get("createdAt")
        else:
            operation.setdefault("attempts", 0)
        if error:
            operation["lastError"] = error
        path = self._pending_path(operation_id)
        write_json_atomic(path, operation)
        return path

    def pending(self, limit: int | None = None) -> list[dict[str, Any]]:
        rows = [self._read(path) for path in sorted(self.pending_dir.glob("*.json"))]
        operations = [row for row in rows if row]
        if limit is not None:
            return operations[:limit]
        return operations

    def mark_done(self, operation: dict[str, Any]) -> None:
        operation_id = str(operation["operationId"])
        source = self._pending_path(operation_id)
        if not source.exists():
            return
        target = self.done_dir / source.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))

    def mark_failed(self, operation: dict[str, Any], error: str) -> None:
        operation = dict(operation)
        operation["attempts"] = int(operation.get("attempts") or 0) + 1
        operation["lastError"] = error
        write_json_atomic(self._pending_path(str(operation["operationId"])), operation)

    def _pending_path(self, operation_id: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in "._:-" else "-" for ch in operation_id)
        return self.pending_dir / f"{safe[:180]}.json"

    @staticmethod
    def _read(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

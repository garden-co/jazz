from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import FlushResult, ListResult, OperationResult
from .outbox import OperationOutbox, stable_json, write_json_atomic
from .transport import SubprocessAgentInfraTransport, Transport, TransportError


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha(value: Any, length: int = 24) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()[:length]


class AgentInfraClient:
    def __init__(
        self,
        *,
        transport: Transport | None = None,
        state_dir: str | Path | None = None,
        fail_safe: bool = True,
        trace_to_jazz: bool = True,
    ) -> None:
        self.transport = transport or SubprocessAgentInfraTransport()
        self.state_dir = Path(
            state_dir
            or os.environ.get("JAZZ2_AGENT_INFRA_STATE_DIR")
            or Path.home() / ".jazz2" / "python-agent-infra"
        ).expanduser()
        self.fail_safe = fail_safe
        self.trace_to_jazz = trace_to_jazz
        self.outbox = OperationOutbox(self.state_dir / "outbox")
        self.trace_dir = self.state_dir / "traces"

    def record_context_digest(self, payload: dict[str, Any]) -> OperationResult:
        prepared = self._prepare_digest_payload(payload)
        operation = self._operation("record-context-digest", prepared)
        started = time.monotonic()
        try:
            record = self.transport.record_context_digest(prepared)
        except TransportError as exc:
            self.outbox.enqueue(operation, error=str(exc))
            trace_path = self._trace(
                operation,
                status="queued",
                duration_ms=self._duration_ms(started),
                error=exc,
            )
            if not self.fail_safe:
                raise
            return OperationResult(
                operation_id=operation["operationId"],
                digest_id=prepared["digestId"],
                status="queued",
                queued=True,
                error=str(exc),
                trace_path=trace_path,
            )
        trace_path = self._trace(operation, status="completed", duration_ms=self._duration_ms(started))
        return OperationResult(
            operation_id=operation["operationId"],
            digest_id=prepared["digestId"],
            status="completed",
            queued=False,
            record=record,
            trace_path=trace_path,
        )

    def queue_context_digest(self, payload: dict[str, Any]) -> OperationResult:
        prepared = self._prepare_digest_payload(payload)
        operation = self._operation("record-context-digest", prepared)
        self.outbox.enqueue(operation)
        trace_path = self._trace(operation, status="queued-local", duration_ms=0)
        return OperationResult(
            operation_id=operation["operationId"],
            digest_id=prepared["digestId"],
            status="queued",
            queued=True,
            trace_path=trace_path,
        )

    def list_context_digests(self, query: dict[str, Any] | None = None) -> ListResult:
        query = dict(query or {})
        operation = self._operation("list-context-digests", query)
        started = time.monotonic()
        try:
            records = self.transport.list_context_digests(query)
        except TransportError as exc:
            trace_path = self._trace(
                operation,
                status="degraded",
                duration_ms=self._duration_ms(started),
                error=exc,
            )
            if not self.fail_safe:
                raise
            return ListResult(records=[], degraded=True, error=str(exc), trace_path=trace_path)
        trace_path = self._trace(operation, status="completed", duration_ms=self._duration_ms(started))
        return ListResult(records=records, degraded=False, trace_path=trace_path)

    def flush_outbox(self, *, limit: int = 50) -> FlushResult:
        completed = 0
        failed = 0
        processed = 0
        seen: set[str] = set()
        while processed < limit:
            pending = [
                operation
                for operation in self.outbox.pending()
                if str(operation.get("operationId") or "") not in seen
            ]
            if not pending:
                break
            operation = pending[0]
            seen.add(str(operation.get("operationId") or ""))
            processed += 1
            started = time.monotonic()
            try:
                self._execute_operation(operation)
            except TransportError as exc:
                failed += 1
                self.outbox.mark_failed(operation, str(exc))
                self._trace(
                    operation,
                    status="retry-failed",
                    duration_ms=self._duration_ms(started),
                    error=exc,
                )
                continue
            completed += 1
            self.outbox.mark_done(operation)
            self._trace(operation, status="replayed", duration_ms=self._duration_ms(started))
        return FlushResult(
            completed=completed,
            failed=failed,
            remaining=len(self.outbox.pending()),
        )

    def pending_operations(self) -> list[dict[str, Any]]:
        return self.outbox.pending()

    def _execute_operation(self, operation: dict[str, Any]) -> Any:
        command = operation.get("command")
        payload = operation.get("payload")
        if command == "record-context-digest" and isinstance(payload, dict):
            return self.transport.record_context_digest(payload)
        raise TransportError(str(command or "unknown"), "unsupported outbox operation")

    def _prepare_digest_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        prepared = {key: value for key, value in payload.items() if value is not None}
        self._require(
            prepared,
            [
                "targetProvider",
                "targetSession",
                "targetTurnOrdinal",
                "targetConversation",
                "targetConversationHash",
                "sourceSession",
                "sourceWatermarkKind",
                "sourceWatermarkValue",
                "kind",
                "digestText",
            ],
        )
        prepared.setdefault("digestId", self._digest_id(prepared))
        return prepared

    @staticmethod
    def _require(payload: dict[str, Any], keys: list[str]) -> None:
        missing = [key for key in keys if payload.get(key) in (None, "")]
        if missing:
            raise ValueError(f"context digest missing required fields: {', '.join(missing)}")

    @staticmethod
    def _digest_id(payload: dict[str, Any]) -> str:
        material = {
            key: payload.get(key)
            for key in [
                "targetProvider",
                "targetSession",
                "targetTurnOrdinal",
                "targetConversationHash",
                "sourceSession",
                "sourceWatermarkKind",
                "sourceWatermarkValue",
                "kind",
                "digestText",
                "modelUsed",
                "score",
                "confidence",
                "reason",
            ]
            if payload.get(key) is not None
        }
        return "ctxdgst_" + sha(material, 32)

    def _operation(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        operation_id = f"jazz2py:{command}:{sha(payload, 32)}"
        return {
            "operationId": operation_id,
            "command": command,
            "payload": payload,
            "createdAt": utc_now(),
            "attempts": 0,
            "suppressJazzTrace": False,
        }

    def _trace(
        self,
        operation: dict[str, Any],
        *,
        status: str,
        duration_ms: int,
        error: TransportError | None = None,
    ) -> Path:
        self.trace_dir.mkdir(parents=True, exist_ok=True)
        path = self.trace_dir / f"{datetime.now(timezone.utc).date().isoformat()}.jsonl"
        payload = operation.get("payload") if isinstance(operation.get("payload"), dict) else {}
        event = {
            "apiVersion": "jazz2.agent-infra-python/v1",
            "kind": "AgentInfraPythonTrace",
            "operationId": operation.get("operationId"),
            "command": operation.get("command"),
            "status": status,
            "durationMs": duration_ms,
            "occurredAt": utc_now(),
            "digestId": payload.get("digestId"),
            "targetSession": payload.get("targetSession"),
            "sourceSession": payload.get("sourceSession"),
        }
        if error:
            event.update(
                {
                    "error": str(error),
                    "errorKind": "timeout" if error.timed_out else "transport",
                    "returncode": error.returncode,
                }
            )
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n")
        write_json_atomic(self.state_dir / "latest-trace.json", event)
        if self.trace_to_jazz and not operation.get("suppressJazzTrace"):
            self._enqueue_jazz_trace(event)
        return path

    def _enqueue_jazz_trace(self, event: dict[str, Any]) -> None:
        digest_payload = {
            "targetProvider": "jazz2-python",
            "targetSession": str(event.get("targetSession") or "jazz2-python"),
            "targetTurnOrdinal": 0,
            "targetConversation": "agent-infra-python-client",
            "targetConversationHash": "agent-infra-python-client",
            "sourceSession": str(event.get("sourceSession") or "jazz2-python"),
            "sourceWatermarkKind": "operation",
            "sourceWatermarkValue": str(event.get("operationId") or "unknown"),
            "kind": "jazz2-python-client-trace",
            "digestText": json.dumps(event, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
            "confidence": "high",
            "reason": "Python agent-infra client operation trace",
            "generatedAt": event.get("occurredAt") or utc_now(),
            "status": "ready" if event.get("status") in {"completed", "replayed"} else "error",
        }
        prepared = self._prepare_digest_payload(digest_payload)
        prepared["digestId"] = "jazz2pytrace_" + sha(
            {
                "operationId": event.get("operationId"),
                "status": event.get("status"),
                "occurredAt": event.get("occurredAt"),
            },
            32,
        )
        operation = self._operation("record-context-digest", prepared)
        operation["suppressJazzTrace"] = True
        self.outbox.enqueue(operation)

    @staticmethod
    def _duration_ms(started: float) -> int:
        return max(0, round((time.monotonic() - started) * 1000))

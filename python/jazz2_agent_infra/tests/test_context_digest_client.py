from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from jazz2_agent_infra import AgentInfraClient, HermesMemoryBridge, TransportError


class MemoryTransport:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail_next: str | None = None
        self.fail_always: str | None = None

    def record_context_digest(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("record-context-digest", payload))
        if self.fail_always == "record" or self.fail_next == "record":
            self.fail_next = None
            raise TransportError("record-context-digest", "simulated record failure")
        record = {
            "eventId": payload["digestId"],
            **payload,
            "generatedAt": payload.get("generatedAt") or "2026-04-27T12:00:00Z",
            "expiresAt": payload.get("expiresAt"),
            "status": payload.get("status") or "ready",
        }
        self.records[payload["digestId"]] = record
        return record

    def list_context_digests(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append(("list-context-digests", query))
        if self.fail_always == "list" or self.fail_next == "list":
            self.fail_next = None
            raise TransportError("list-context-digests", "simulated list failure")
        records = list(self.records.values())
        if target := query.get("targetSession"):
            records = [record for record in records if record.get("targetSession") == target]
        if kind := query.get("kind"):
            records = [record for record in records if record.get("kind") == kind]
        return records[: int(query.get("limit") or 20)]


def sample_digest(text: str = "The CAD shell loaded the shared canvas.") -> dict[str, Any]:
    return {
        "targetProvider": "hermes",
        "targetSession": "hermes:alice",
        "targetTurnOrdinal": 7,
        "targetConversation": "designer-cad-demo",
        "targetConversationHash": "conv-hash-1",
        "sourceSession": "codex:019d-demo",
        "sourceWatermarkKind": "turn",
        "sourceWatermarkValue": "turn-42",
        "kind": "context-pass",
        "digestText": text,
        "confidence": "high",
        "reason": "validated by local Designer smoke",
    }


class AgentInfraClientTests(unittest.TestCase):
    def test_records_context_digest_with_stable_idempotency_key_and_trace(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            client = AgentInfraClient(transport=transport, state_dir=Path(temp), trace_to_jazz=False)

            first = client.record_context_digest(sample_digest())
            second = client.record_context_digest(sample_digest())

            self.assertEqual(first.status, "completed")
            self.assertEqual(second.status, "completed")
            self.assertFalse(first.queued)
            self.assertEqual(first.digest_id, second.digest_id)
            self.assertEqual(len(transport.records), 1)
            self.assertTrue(first.digest_id.startswith("ctxdgst_"))
            self.assertTrue((Path(temp) / "traces").exists())
            trace_text = "\n".join(path.read_text() for path in (Path(temp) / "traces").glob("*.jsonl"))
            self.assertIn(first.operation_id, trace_text)
            self.assertIn('"status":"completed"', trace_text)

    def test_failed_record_is_queued_and_replayed_without_losing_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            transport.fail_next = "record"
            client = AgentInfraClient(transport=transport, state_dir=Path(temp), trace_to_jazz=False)

            queued = client.record_context_digest(sample_digest("queued for replay"))

            self.assertEqual(queued.status, "queued")
            self.assertTrue(queued.queued)
            self.assertEqual(len(client.pending_operations()), 1)

            flushed = client.flush_outbox()

            self.assertEqual(flushed.completed, 1)
            self.assertEqual(flushed.failed, 0)
            self.assertEqual(len(client.pending_operations()), 0)
            self.assertIn(queued.digest_id, transport.records)

    def test_queue_context_digest_does_not_call_transport_until_flush(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            client = AgentInfraClient(transport=transport, state_dir=Path(temp), trace_to_jazz=False)

            queued = client.queue_context_digest(sample_digest("queue first"))

            self.assertEqual(queued.status, "queued")
            self.assertEqual(transport.calls, [])
            self.assertEqual(len(client.pending_operations()), 1)

            flushed = client.flush_outbox()

            self.assertEqual(flushed.completed, 1)
            self.assertEqual(len(transport.records), 1)

    def test_replay_retains_failed_operations_with_attempt_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            transport.fail_always = "record"
            client = AgentInfraClient(transport=transport, state_dir=Path(temp), trace_to_jazz=False)

            client.record_context_digest(sample_digest("still down"))
            flushed = client.flush_outbox()
            pending = client.pending_operations()

            self.assertEqual(flushed.completed, 0)
            self.assertEqual(flushed.failed, 1)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["attempts"], 1)
            self.assertIn("simulated record failure", pending[0]["lastError"])

    def test_list_context_digests_is_fail_safe_and_traced(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            transport.fail_always = "list"
            client = AgentInfraClient(
                transport=transport,
                state_dir=Path(temp),
                fail_safe=True,
                trace_to_jazz=False,
            )

            result = client.list_context_digests({"targetSession": "hermes:alice"})

            self.assertEqual(result.records, [])
            self.assertTrue(result.degraded)
            self.assertIn("simulated list failure", result.error or "")
            trace_text = "\n".join(path.read_text() for path in (Path(temp) / "traces").glob("*.jsonl"))
            self.assertIn('"status":"degraded"', trace_text)

    def test_list_context_digests_can_raise_in_strict_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            transport.fail_always = "list"
            client = AgentInfraClient(
                transport=transport,
                state_dir=Path(temp),
                fail_safe=False,
                trace_to_jazz=False,
            )

            with self.assertRaises(TransportError):
                client.list_context_digests({"targetSession": "hermes:alice"})

    def test_operation_trace_can_be_materialized_as_jazz_context_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            client = AgentInfraClient(transport=transport, state_dir=Path(temp), trace_to_jazz=True)

            result = client.record_context_digest(sample_digest("trace this write"))
            pending = client.pending_operations()

            self.assertEqual(result.status, "completed")
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["payload"]["kind"], "jazz2-python-client-trace")

            flushed = client.flush_outbox()

            self.assertEqual(flushed.completed, 1)
            trace_records = [
                record
                for record in transport.records.values()
                if record.get("kind") == "jazz2-python-client-trace"
            ]
            self.assertEqual(len(trace_records), 1)
            self.assertIn(result.operation_id, trace_records[0]["digestText"])
            self.assertEqual(len(client.pending_operations()), 0)

    def test_queue_first_flush_drains_generated_trace_operations(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            client = AgentInfraClient(transport=transport, state_dir=Path(temp), trace_to_jazz=True)

            client.queue_context_digest(sample_digest("queue and trace"))
            flushed = client.flush_outbox()

            self.assertEqual(flushed.completed, 3)
            self.assertEqual(flushed.remaining, 0)
            self.assertEqual(len(client.pending_operations()), 0)
            self.assertEqual(
                len(
                    [
                        record
                        for record in transport.records.values()
                        if record.get("kind") == "jazz2-python-client-trace"
                    ]
                ),
                2,
            )

    def test_hermes_bridge_maps_memory_writes_and_local_search(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            transport = MemoryTransport()
            client = AgentInfraClient(transport=transport, state_dir=Path(temp), trace_to_jazz=False)
            bridge = HermesMemoryBridge(client)

            stored = bridge.store_memory(
                target_session="hermes:cad-demo",
                conversation="designer-cad-demo",
                turn_ordinal=3,
                source_session="codex:019d-cad",
                source_watermark="turn-3",
                text="Build123d generated a bracket with two counterbored holes.",
                confidence="high",
            )
            matches = bridge.search_memory(
                target_session="hermes:cad-demo",
                query="counterbored bracket",
                limit=3,
            )

            self.assertEqual(stored.status, "completed")
            self.assertEqual(len(matches.records), 1)
            self.assertIn("counterbored holes", matches.records[0]["digestText"])


if __name__ == "__main__":
    unittest.main()

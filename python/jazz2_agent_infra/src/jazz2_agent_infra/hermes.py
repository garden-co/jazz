from __future__ import annotations

import hashlib
import re
from typing import Any

from .client import AgentInfraClient
from .models import ListResult, OperationResult


def conversation_hash(value: str) -> str:
    return "hermesconv_" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


class HermesMemoryBridge:
    """Hermes-facing convenience layer over ContextDigest records.

    This module intentionally does not import Hermes internals. The Hermes
    provider can use it as a stable adapter while keeping lifecycle hooks inside
    Hermes.
    """

    def __init__(self, client: AgentInfraClient) -> None:
        self.client = client

    def store_memory(
        self,
        *,
        target_session: str,
        conversation: str,
        turn_ordinal: int,
        source_session: str,
        source_watermark: str,
        text: str,
        kind: str = "hermes-memory",
        confidence: str = "medium",
        reason: str = "Hermes memory write",
        model_used: str | None = None,
        expires_at: str | None = None,
    ) -> OperationResult:
        payload: dict[str, Any] = {
            "targetProvider": "hermes",
            "targetSession": target_session,
            "targetTurnOrdinal": turn_ordinal,
            "targetConversation": conversation,
            "targetConversationHash": conversation_hash(conversation),
            "sourceSession": source_session,
            "sourceWatermarkKind": "turn",
            "sourceWatermarkValue": source_watermark,
            "kind": kind,
            "digestText": text,
            "confidence": confidence,
            "reason": reason,
        }
        if model_used:
            payload["modelUsed"] = model_used
        if expires_at:
            payload["expiresAt"] = expires_at
        return self.client.record_context_digest(payload)

    def search_memory(
        self,
        *,
        target_session: str,
        query: str,
        limit: int = 8,
        kind: str = "hermes-memory",
    ) -> ListResult:
        result = self.client.list_context_digests(
            {
                "targetSession": target_session,
                "kind": kind,
                "limit": max(limit * 8, 20),
            }
        )
        if result.degraded:
            return result
        ranked = sorted(
            result.records,
            key=lambda record: self._score(query, record),
            reverse=True,
        )
        return ListResult(records=ranked[:limit], degraded=False, trace_path=result.trace_path)

    @staticmethod
    def _score(query: str, record: dict[str, Any]) -> int:
        haystack = " ".join(
            str(record.get(key) or "")
            for key in ["digestText", "reason", "kind", "sourceSession"]
        ).lower()
        terms = [term for term in re.split(r"\W+", query.lower()) if term]
        return sum(1 for term in terms if term in haystack)

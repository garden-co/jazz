# Jazz2 Agent Infra Python

Typed Python client for the Jazz2 `examples/agent-infra-backend` control
plane.

The client follows the same binding shape as the Go package: it stays thin and
talks to the stable app/backend API instead of wrapping the Rust runtime or
embedding TypeScript snippets.

## Properties

- deterministic context digest IDs for store-level idempotency
- fail-safe writes through a durable filesystem outbox
- fast degraded reads when the backend is unavailable
- local JSONL traces for every operation
- optional Jazz2-materialized operation traces as context digests
- subprocess transport over the built `agent-infra-backend` CLI

## Hermes Shape

Hermes should construct one long-lived client and call:

```python
from jazz2_agent_infra import AgentInfraClient, HermesMemoryBridge

client = AgentInfraClient()
client.flush_outbox()
memory = HermesMemoryBridge(client)
result = memory.store_memory(
    target_session="hermes:session",
    conversation="designer-demo",
    turn_ordinal=1,
    source_session="codex:session",
    source_watermark="turn-1",
    text="User asked for a CAD canvas backed by build123d.",
)
```

For the hottest path, enqueue first and let a background worker flush:

```python
client.queue_context_digest({...})
client.flush_outbox()
```

If Jazz2 is down, `record_context_digest` returns a queued result and the
payload remains in `~/.jazz2/python-agent-infra/outbox/pending` until
`flush_outbox()` can replay it.

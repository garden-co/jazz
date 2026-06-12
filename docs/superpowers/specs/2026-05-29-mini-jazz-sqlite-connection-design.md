# mini-jazz-sqlite Connection Protocol Design

## Context

`mini-jazz-sqlite` currently syncs by directly exporting and applying
`sync::Bundle` values between `Runtime` instances. Tests and harness helpers
manually call export methods on one runtime and `apply_bundle` on another.

The next step is a connection abstraction that lets the same sync protocol run
over in-memory tests, browser Worker communication, and network transports. The
first version should introduce protocol-level session semantics:
subscribe, replay, settled, and close.

The protocol is explicitly client-to-upstream. The same downstream/upstream
contract can be composed for tab to worker, worker to edge, and edge to global
authority.

## Goals

- Define a transport-neutral connection contract for sync sessions.
- Keep `sync::Bundle` as the semantic data payload for the first version.
- Add typed session messages for handshake, subscribe, replay, data, settled,
  errors, acknowledgements, and close.
- Keep Worker and network transport details outside the Rust core.
- Make session behavior deterministic and testable with in-memory connections.
- Preserve current bundle idempotence and fail-closed compatibility checks.

## Non-Goals

- Replacing the `Bundle` wire payload immediately.
- Adding real Worker or WebSocket transports in the first implementation.
- Moving async runtimes or transport dependencies into `mini-jazz-sqlite`.
- Persisting active query descriptors as durable application data.
- Designing final catalogue, blob, compression, or auth protocols.

## Architecture

Add a protocol layer inside `crates/mini-jazz-sqlite`, separate from
`sync::Bundle` and `Runtime`.

`sync::Bundle` remains the data frame. The protocol layer wraps it in session
messages:

- downstream to upstream: `Hello`, `Subscribe`, `Replay`, `Ack`, `Close`
- upstream to downstream: `Hello`, `Data`, `Settled`, `Error`, `Close`

A connection implementation is responsible for reliable ordered delivery of
typed messages. It does not implement query semantics, policy, bundle assembly,
or bundle application.

`Runtime` remains the semantic executor. It exports query scopes, applies
bundles, records observed query reads, and computes subscription deltas. The
protocol layer coordinates when those runtime operations happen across a
session.

## Components

### `protocol`

Pure serializable types:

- `ClientMessage`
- `ServerMessage`
- `SessionId`
- `SubscriptionId`
- `MessageId`
- `ReplayCursor`
- `SettlementTier`
- `ProtocolVersion`
- `ProtocolCapabilities`
- `ProtocolError`
- `CloseReason`

These types should be simple Rust data structures with `serde` support so they
can cross Rust tests, WASM bindings, Worker messages, and network framing.

### `connection`

A small driver-facing contract for moving typed messages. The core contract
should stay synchronous and dependency-light so the crate does not need an async
runtime. Worker, WebSocket, or native network adapters can translate their own
async events into calls against this contract.

The in-core implementation should include an in-memory connection pair for
tests. It should prove message ordering, disconnect, reconnect, replay, and
close behavior without requiring platform APIs.

### `session`

A deterministic client-to-upstream state machine. It tracks:

- handshake status
- active subscriptions
- replay cursors
- pending outbound messages
- acknowledged inbound messages
- settlement state by subscription and tier
- close state

The state machine should not know about Worker, WebSocket, or filesystem
details.

### Runtime Protocol Glue

Small functions bridge session events to existing runtime operations:

- `Subscribe` on upstream exports the requested query scope.
- `Replay` on upstream exports refreshes for active downstream interest.
- `Data` on downstream applies a `Bundle`.
- `Settled` on downstream marks a query/tier delivery barrier.
- `Close` drops session-local interest and settlement state.

## Message Semantics

### Handshake

Downstream sends:

```text
Hello {
  protocol_version,
  node_id,
  schema_fingerprint,
  policy_fingerprint
}
```

Upstream replies with accepted protocol version, upstream node id, and
capabilities. If the protocol, schema, policy, or auth context is incompatible,
upstream sends `Error` and then `Close`.

### Subscribe

Downstream sends:

```text
Subscribe {
  subscription_id,
  query,
  requested_tier
}
```

The query should use the existing `BuiltQuery` descriptor shape first. Upstream
exports one or more `Data` frames carrying `Bundle` payloads. After all data
needed for the requested delivery tier has been sent, upstream sends `Settled`.

Downstream may publish a settled subscription result only after it has applied
the matching data cursor and observed the corresponding `Settled` frame.

### Replay

After reconnect, downstream sends active subscriptions and the last applied
cursor known for each subscription.

Upstream may satisfy replay by sending missing data from a replay window. If the
window is insufficient, upstream sends fresh query-scope bundles for those
subscriptions. Existing bundle idempotence makes repeated or overlapping data
safe to apply.

### Data

`Data` carries a `Bundle`, optional `subscription_id`, and cursor metadata. A
data frame may represent subscription refresh data, write forwarding data, or
authoritative outcome/receipt data.

The first implementation can keep write forwarding as bundle data and reuse
existing trusted or untrusted apply paths based on session role. A later design
can introduce dedicated mutation frames if needed.

### Acknowledgement

`Ack` records the highest applied message or cursor. It gives upstream enough
state to bound replay windows and lets tests prove reconnect behavior. Acking a
message means the receiver has durably applied its semantic effects or has
intentionally ignored an idempotent duplicate.

### Settled

`Settled` is a delivery barrier, not a row payload. It means the upstream has
sent the data needed for a subscription at a requested tier and cursor.

Settlement is tracked per subscription and tier. Rows may arrive before
settlement, but the downstream must not publish them as the settled tier result
until the barrier is observed.

### Close

Either side may send `Close`. After close, both sides stop accepting new
subscribe, replay, data, and ack messages for that session. Runtime data remains
cached; only session-local desired interest, replay state, and settlement state
are dropped.

## Error Handling

Protocol errors are explicit frames:

```text
Error {
  code,
  message,
  subscription_id?,
  message_id?,
  retry_hint
}
```

Retry hints are:

- `retryable`
- `replay_required`
- `fatal`

Fatal errors close the session after the error frame. Examples:

- incompatible protocol version
- incompatible schema fingerprint
- incompatible policy fingerprint
- auth failure
- malformed message
- unsupported required capability

Scoped errors may fail one subscription without closing the session. Examples:

- unknown subscription id
- rejected query descriptor
- expired replay cursor
- settlement timeout
- bundle apply failure scoped to one subscription

Transport failures are outside the core protocol. Adapters translate them into
disconnect/reconnect events. The downstream session then reconnects with
`Replay`.

Bundle application remains fail-closed. Unsupported bundle protocol versions or
incompatible fingerprints must not partially apply.

## Testing

Use high-level integration tests under `crates/mini-jazz-sqlite/tests/whole_system`.
Prefer realistic tab, worker, edge, and authority roles over generic peers.

Initial tests:

1. `connection_subscribe_delivers_initial_query_and_settled`
   - Alice creates rows upstream.
   - A tab subscribes through an in-memory connection.
   - The tab applies data, receives settled, and sees the expected query result.

2. `connection_replay_restores_active_subscription_after_reconnect`
   - A tab subscribes and settles.
   - The connection disconnects.
   - Upstream changes data.
   - The tab reconnects with replay state and converges after missing or
     refreshed data is sent.

3. `connection_close_drops_session_interest_without_deleting_cached_rows`
   - A tab receives data for a subscription.
   - The session closes.
   - Active interest is gone, but previously synced rows remain queryable from
     local cache.

4. `connection_rejects_incompatible_protocol_without_partial_apply`
   - A future protocol version or incompatible fingerprint produces error and
     close.
   - The downstream runtime remains unchanged.

5. `connection_can_run_tab_to_worker_and_worker_to_authority_with_same_messages`
   - A two-hop in-memory topology uses the same message types for both hops.
   - Data converges without any Worker-specific or network-specific code.

## Implementation Shape

The first implementation should be small and vertical:

1. Add serializable protocol message types.
2. Add in-memory connection test support.
3. Add session state for handshake, subscribe, data, settled, replay, ack, and
   close.
4. Add runtime glue that maps subscribe/replay/data events onto existing
   `Runtime` bundle export/apply methods.
5. Add whole-system tests proving subscribe, replay, close, incompatibility,
   and two-hop composition.

The implementation should not change existing application-facing query/write
APIs. New public surface should be protocol-oriented and clearly separate from
ordinary app methods.

## Open Follow-Up Work

- Worker `postMessage` adapter in the WASM/JS layer.
- WebSocket or other network adapter.
- Dedicated mutation frames for write forwarding if bundle data frames are too
  broad.
- Replay window storage and eviction policy.
- Capability negotiation for compression, binary framing, catalogue lanes, and
  blob transfer.
- Product-facing connection API once the protocol state machine is proven.

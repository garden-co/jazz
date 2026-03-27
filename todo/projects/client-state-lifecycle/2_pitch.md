# Pitch: Client State Lifecycle

## Problem

Server-side client state (`ClientState`, query subscriptions, sync cursors) accumulates forever because there is no path from "disconnected" to "cleaned up." In a multi-tenant cloud, orphaned state across many apps grows without bound — both memory and wasted CPU computing sync updates for ghost clients.

## Solution

### Lifecycle model

```
Connected ──(stream close)──▶ Disconnected ──(TTL expires)──▶ Reaped
    ▲                              │
    └──────(reconnect)─────────────┘
```

The SSE layer already knows when clients connect and disconnect. We use this to maintain a **disconnect candidates set** — a timestamped map of clients that have lost their SSE stream but haven't been reaped yet.

### Disconnect candidates set

A new field on `ServerState`:

```rust
disconnect_candidates: RwLock<HashMap<ClientId, Instant>>
```

Three operations, all in the server layer (`routes.rs` / `ServerState`):

- **SSE stream closes** → insert `(client_id, Instant::now())`
- **Client reconnects** (new SSE stream with same `client_id`) → remove from candidates. State preserved, incremental sync resumes.
- **Sweep fires** → drain entries where `disconnected_at + ttl < now`, call `runtime.remove_client(id)` for each.

No changes to `ClientState`, `SyncManager`, or `RuntimeCore` (beyond `remove_client`, which is already done).

### Periodic sweep

A `tokio::time::interval(Duration::from_secs(30))` background task in the server layer:

1. Read `disconnect_candidates`, collect entries past TTL
2. Call `runtime.remove_client(id)` for each expired entry
3. Remove reaped entries from candidates
4. Log reaped client IDs at `debug` level

Each `remove_client` call is the same code path as any other client removal — no new method on RuntimeCore.

### Per-app TTL configuration

Default TTL: **5 minutes**. Stored as `client_ttl: Arc<AtomicU64>` (millis) in `ServerState`. The sweep reads it each tick, so runtime changes take effect within 30 seconds. Each app's server instance gets its own config in the managed cloud.

### Reconnect after reaping

A reaped client that reconnects gets a fresh `ClientState` via the existing `add_client` / `ensure_client_with_session` path. It must re-subscribe to queries and will receive a full initial sync. No protocol changes needed — this is indistinguishable from a new client connecting.

### Invariants

- **INV-L1**: A client with an active SSE connection is never in the candidates set and is never reaped.
- **INV-L2**: A disconnected client's state survives for at least `client_ttl` after disconnect (up to `client_ttl + 30s` in practice).
- **INV-L3**: After reaping, no per-client state remains in SyncManager or QueryManager (enforced by existing `remove_client` tests).
- **INV-L4**: A reaped client that reconnects is indistinguishable from a new client.

## Rabbit Holes

### 1. Sweep vs runtime mutex contention

Each `remove_client` call acquires the `Arc<Mutex<RuntimeCore>>` — the same lock that `batched_tick` and `push_sync_inbox` use. The sweep must not hold many of these in sequence without yielding.

**Solution:** The sweep calls `remove_client` one at a time, yielding between each (`tokio::task::yield_now()` or just the natural `.await` on the mutex). Each individual call is ~microseconds. This interleaves with normal sync operations rather than blocking them.

### 2. Race between reconnect and sweep

A client could reconnect at the exact moment the sweep is processing it. Without care, the sweep could reap a just-reconnected client.

**Solution:** Use an atomic check-then-act pattern on the candidates set:

```
Sweep:
  1. Write-lock disconnect_candidates
  2. Drain expired entries into a local Vec<ClientId>
  3. Unlock disconnect_candidates
  4. For each expired client_id, call runtime.remove_client(id)

Reconnect (events_handler):
  1. Write-lock disconnect_candidates
  2. Remove client_id from candidates (if present)
  3. Unlock disconnect_candidates
  4. Call ensure_client_with_session
```

The key: the sweep removes entries from the candidates map _before_ calling `remove_client`, and **guards each reap with a connection check**:

```
Sweep:
  1. Write-lock disconnect_candidates
  2. Drain expired entries into a local Vec<ClientId>
  3. Unlock disconnect_candidates
  4. For each expired client_id:
     a. Read-lock connections
     b. Check if any connection has this client_id
     c. If connected → skip (client reconnected between drain and reap)
     d. If not connected → call runtime.remove_client(id)
```

This eliminates the dangerous race: if a reconnect arrives between steps 3 and 4, the new connection is visible in the `connections` map, and step 4b catches it.

- **Reconnect wins the race**: Step 4b sees the new connection → skip. Client state is preserved. Clean.
- **Sweep wins the race**: No connection exists at step 4b → `remove_client` runs. If the client reconnects afterward, `ensure_client_with_session` creates fresh state via `add_client`. Clean.

### 3. Multiple SSE connections per client_id

The current code doesn't prevent two simultaneous SSE streams with the same `client_id` (e.g., two browser tabs). If one tab closes, naively inserting into candidates would mark the client for reaping even though the other tab is still connected.

**Solution:** Track a **connection count** per `client_id`. The `connections: HashMap<u64, ConnectionState>` already stores `_client_id` on each connection. On stream close:

```
Stream close:
  1. Remove this connection_id from connections
  2. Check if any remaining connection in the map has this client_id
  3. If no remaining connections → insert into disconnect_candidates
  4. If other connections exist → do nothing (client still connected)
```

This is a scan of the connections map, which is small (only active SSE streams). The `_client_id` field on `ConnectionState` is already there — just unused. We rename it to `client_id` and use it.

Alternatively, maintain a separate `active_connection_count: HashMap<ClientId, u32>` on `ServerState` for O(1) lookup instead of scanning. Given the connections map is small, the scan is fine for now — optimize later if needed.

## No-gos

- **Explicit `Disconnect` protocol message.** Deferred — the `beforeunload` path in browsers is unreliable and adds protocol surface area. TTL-only is sufficient for now.
- **Persisting client state across server restarts.** `ClientState` is in-memory only. Server restart already loses all client state; clients re-subscribe on reconnect. This is acceptable.
- **Per-client TTL.** TTL is per-app, not per-client. No use case for client-specific retention.
- **Graceful "you were reaped" notification.** No server-to-client signal. The reconnect path handles this transparently.

## Testing Strategy

Integration tests at the server layer (routes / ServerState level) with realistic actors.

### Core lifecycle

- **alice connects, disconnects, TTL expires → state is reaped.** Verify `ClientState` is gone from SyncManager.
- **alice connects, disconnects, reconnects before TTL → state preserved.** Verify sent_tips and query subscriptions survive.
- **alice and bob connected, alice disconnects and is reaped → bob unaffected.** Verify bob's subscriptions and state intact.
- **alice disconnects, reconnects after reaping → fresh state.** Verify alice gets a new `ClientState` and must re-subscribe.
- **Sweep processes multiple expired candidates in one tick.** Verify all are reaped, none left behind.

### Multi-connection

- **alice has two SSE streams, one closes → not added to candidates.** Verify alice stays connected.
- **alice has two SSE streams, both close → added to candidates.** Verify alice is reaped after TTL.

### Race condition

The race between reconnect and sweep is tested deterministically by decomposing into the two possible orderings — no threads or timing tricks needed:

- **Sweep wins:** Drain candidates, verify no active connection for alice, call `remove_client`, then simulate reconnect via `ensure_client_with_session`. Assert: alice has a fresh `ClientState`, is not in candidates, can subscribe to queries.
- **Reconnect wins:** Drain candidates (alice is in the drained set), then simulate reconnect by adding a connection and calling `ensure_client_with_session`. Now the sweep checks connections for alice — finds one → skips reap. Assert: alice's `ClientState` is preserved with existing sent_tips and query subscriptions.

The key property: **after the race resolves, the client is either connected with valid state or cleanly absent.** No half-states, no reaping a connected client.

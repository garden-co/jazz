# Client State Lifecycle

Server-side garbage collection of per-client state for permanently disconnected clients.

## Context

Clients connect via SSE (`GET /events`) and mutate via `POST /sync`. Each client provides a persistent `client_id` (UUIDv7) that survives reconnects. On disconnect, the server retains all logical client state (`ClientState`, query subscriptions, sent_tips, policy checks) so that a reconnecting client can resume incrementally without a full re-sync.

This is correct for transient disconnects (WiFi blip, tab switch), but clients that are permanently gone (uninstall, cleared browser data, abandoned session) leave orphaned state that accumulates. In a multi-tenant cloud deployment with many apps, this compounds into a real resource problem (~20KB per client across subscriptions, sync cursors, and metadata tracking).

### What's already done

`remove_client` (on branch `gio/sub-disconnect-cleanup`) now thoroughly drains all per-client state:

- **SyncManager**: `clients`, `commit_interest`, `query_origin`, `pending_permission_checks`, `pending_query_subscriptions`, `pending_query_unsubscriptions`, `inbox`, `outbox`
- **QueryManager**: `server_subscriptions`, `active_policy_checks`
- **RuntimeCore**: routes through `QueryManager::remove_client`

The missing piece is _when_ to call `remove_client`.

## Design

### Client state lifecycle

```
Connected ──(stream close)──▶ Disconnected ──(TTL expires)──▶ Reaped
    ▲                              │
    └──────(reconnect)─────────────┘

Connected ──(Disconnect msg)──▶ Reaped  (explicit goodbye, no TTL wait)
```

**Connected**: Active SSE stream. `last_seen` updated on client registration and every inbound message.

**Disconnected**: SSE stream dropped, `ClientState` retained. `last_seen` frozen at time of last inbound message. Client can reconnect with same `client_id` and resume (sent_tips, queries, session preserved).

**Reaped**: Either TTL expired or client sent explicit `Disconnect`. `remove_client` called — all per-client state drained. If the client reconnects after reaping, it gets a fresh `ClientState` via the existing `add_client` / `ensure_client_with_session` path and must re-subscribe to queries.

### Protocol addition

One new `SyncPayload` variant:

```rust
SyncPayload::Disconnect
```

Sent by the client via `POST /sync` when it knows it's leaving (tab close via `beforeunload`, app exit, explicit logout). The server calls `remove_client` immediately — no TTL wait.

This is best-effort. Clients that crash, lose network, or get uninstalled never send it — that's what the TTL fallback covers.

No server-to-client "you were reaped" message. Reconnect after reaping follows the existing connection flow.

### Tracking: `last_seen` on `ClientState`

New field:

```rust
pub last_seen: Instant
```

Updated when:

- Client is registered (`add_client`, `ensure_client_with_session`)
- Server receives any `InboxEntry` from `Source::Client(id)` during `process_inbox`

Not updated on outbound messages — those don't prove the client is alive.

### Periodic sweep

New method on `RuntimeCore`:

```rust
pub fn reap_stale_clients(
    &mut self,
    ttl: Duration,
    connected_clients: &HashSet<ClientId>,
) -> Vec<ClientId>
```

Walks `SyncManager.clients`, skips clients present in `connected_clients` (those with an active SSE stream — they're alive even if idle), and calls `remove_client` for entries where `last_seen + ttl < now`. Returns the list of reaped client IDs for logging/metrics.

The `connected_clients` set is derived from the `connections` map in the server layer, which already tracks active SSE streams. This keeps transport awareness out of `RuntimeCore` — it only sees a set of "don't reap these" IDs.

### Server-layer integration

A `tokio::time::interval(Duration::from_secs(30))` background task:

1. Reads `ServerState.connections` to collect currently-connected client IDs
2. Calls `runtime.reap_stale_clients(ttl, &connected_clients)`
3. Logs reaped client IDs at `debug` level

The `sync_handler` recognizes `SyncPayload::Disconnect` and calls `remove_client` directly instead of pushing to the inbox.

The `events_handler` stream close path is unchanged — it still retains client state for reconnect.

### Per-app TTL configuration

Default TTL: **5 minutes**.

Stored as `client_ttl: Arc<AtomicU64>` (millis) in `ServerState`. The sweep reads it on each tick, so changes take effect within 30 seconds. Runtime-adjustable via admin API or config reload — no restart required.

Each app's server instance gets its own config in the managed cloud, so per-app TTL is per-deployment configuration.

## Invariants

- **INV-L1**: A client with an active SSE connection is never reaped, regardless of `last_seen`.
- **INV-L2**: A disconnected client's state survives for at least `client_ttl` after its last inbound message (up to `client_ttl + sweep_interval` in practice).
- **INV-L3**: After reaping, no per-client state remains in SyncManager or QueryManager (enforced by existing `remove_client` tests).
- **INV-L4**: A reaped client that reconnects is indistinguishable from a new client — fresh `ClientState`, must re-subscribe.

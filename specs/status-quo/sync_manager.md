# Sync Manager — Status Quo

The Sync Manager coordinates data flow between nodes in the multi-tier topology (browser ↔ edge server ↔ global server). Its job is to ensure each node has the data it needs — no more, no less.

The fundamental asymmetry: **upward** (to servers), we push everything — the server is trusted and needs all data for query evaluation. **Downward** (to clients), we push only what matches the client's active queries — clients are untrusted and shouldn't see data they haven't asked for.

This component is a state machine, not a query engine. It tracks connections, roles, scopes, and message queues. The actual query evaluation happens in the [Query Manager](query_manager.md) — see [Query/Sync Integration](query_sync_integration.md) for how they coordinate. The [Object Manager](object_manager.md) provides the underlying data, and the [HTTP Transport](http_transport.md) carries the messages over the network.

## Connection Types

|             | Upstream Servers | Downstream Clients           |
| ----------- | ---------------- | ---------------------------- |
| Trust       | Trusted          | Untrusted                    |
| Scope       | All objects      | Query-filtered               |
| Direction   | Bidirectional    | Push to them, they push back |
| Permissions | Full access      | Role-based (User/Admin/Peer) |

### Server Model

- **Upward** (us → server): push ALL objects, forward client queries
- **Downward** (server → us): server sends updates matching forwarded queries

### Client Roles

Roles replace the earlier scope-based permission model. The old model checked "is this object in the client's readable/writable scope?" per-object, which was complex and had a security bypass bug. Roles are simpler: the role determines the code path, and fine-grained permissions happen via ReBAC policy evaluation for User writes.

| Role    | Capabilities                                                                                |
| ------- | ------------------------------------------------------------------------------------------- |
| `User`  | Requires session. Writes queued for ReBAC permission check. Cannot write catalogue objects. |
| `Admin` | Direct apply, no permission check needed                                                    |
| `Peer`  | Direct apply, used for server-to-server sync                                                |

> `crates/groove/src/sync_manager.rs` — ClientRole at struct definition

## State Structures

### ServerState

Tracks what we've sent to each server: `sent_tips`, `sent_metadata`.

> `crates/groove/src/sync_manager.rs:115-120`

### ClientState

Tracks per-client: `role`, `session`, `queries` (HashMap<QueryId, QueryScope>), `sent_tips`, `sent_metadata`.

The effective scope is computed dynamically via `is_in_scope()` rather than stored as a materialized field.

> `crates/groove/src/sync_manager.rs:133-144`

### DurabilityTier

```
Worker | EdgeServer | GlobalServer
```

Used in PersistenceAck and QuerySettled for tier-aware durability.

> `crates/groove/src/sync_manager.rs:26-30`

## Message Protocol

### SyncPayload

| Variant                                     | Purpose                                      |
| ------------------------------------------- | -------------------------------------------- |
| `ObjectUpdated`                             | Object/branch commits (topologically sorted) |
| `ObjectTruncated`                           | Branch truncation notification               |
| `QuerySubscription` / `QueryUnsubscription` | Client query registration                    |
| `PersistenceAck`                            | Tier-level durability confirmation           |
| `QuerySettled`                              | Query results settled at tier                |
| `Error(SyncError)`                          | Error response                               |

> `crates/groove/src/sync_manager.rs:205-254`

Note: the spec originally called these `QueryRegistration`/`QueryUnregistration` — renamed to `QuerySubscription`/`QueryUnsubscription` in implementation.

`QuerySubscription` now carries `propagation` (`full` default, `local-only` optional). `local-only` prevents forwarding beyond the local durability tier.

> [`sync_manager/types.rs:83`](../../crates/jazz-tools/src/sync_manager/types.rs#L83)
> [`sync_manager/types.rs:235`](../../crates/jazz-tools/src/sync_manager/types.rs#L235)

### SyncError

| Variant                | Purpose                                |
| ---------------------- | -------------------------------------- |
| `PermissionDenied`     | Insufficient permission                |
| `SessionRequired`      | User client without session            |
| `CatalogueWriteDenied` | User client attempting catalogue write |

> `crates/groove/src/sync_manager.rs:169-190`

## Public API

### Connection Management

`add_server()`, `remove_server()`, `add_client()`, `remove_client()`, `set_client_role()`.

Adding a server triggers `queue_full_sync_to_server()` — pushes all existing objects.

Query subscription replay on upstream reconnect is intentionally owned by QueryManager (via `QueryManager::add_server()`), not SyncManager. SyncManager remains scope-based and only sends query messages it is asked to send.

**Design decision**: downward sync is always query-scoped. Clients receive data ONLY via query subscriptions — no "full dump" path.

> `crates/groove/src/sync_manager.rs:482-510`
> `crates/groove/src/query_manager/subscriptions.rs:190-240`

### Client Session Management

`ensure_client_with_session()` — idempotent registration that upgrades session (None→Some) without resetting role. Handles SSE + /sync arriving in any order.

> `crates/groove/src/sync_manager.rs`

### Query Handoff to QueryManager

A key boundary: the SyncManager never touches query graphs or SQL. When a client subscribes to a query, the SyncManager queues it and exposes it via `take_pending_query_subscriptions()`. The [Query Manager](query_manager.md) picks it up during `process()`, evaluates the query, and tells the SyncManager what objects matched via `set_client_query_scope()`. This keeps SQL complexity out of the sync layer.

| Method                                   | Purpose                                         |
| ---------------------------------------- | ----------------------------------------------- |
| `take_pending_query_subscriptions()`     | Returns pending subscriptions for QM processing |
| `set_client_query_scope()`               | Called by QM after graph building               |
| `requeue_pending_query_subscriptions()`  | Re-queue if schema unavailable                  |
| `take_pending_query_unsubscriptions()`   | For QM cleanup                                  |
| `send_query_subscription_to_servers()`   | Push queries upstream (honors propagation mode) |
| `send_query_unsubscription_to_servers()` | Remove queries upstream                         |

> `crates/groove/src/sync_manager.rs:605-696`

### Permission Checks

| Method                             | Purpose                                      |
| ---------------------------------- | -------------------------------------------- |
| `take_pending_permission_checks()` | Returns User writes pending ReBAC evaluation |
| `approve_permission_check()`       | Apply approved write                         |
| `reject_permission_check()`        | Send error to client                         |

`PendingPermissionCheck` includes: id, client_id, payload, session, metadata, old_content, new_content, operation — more context than the original "pending updates" design.

> `crates/groove/src/sync_manager.rs:735-755`, `342-355`

## Processing Flow

### Local Change → Outbox

`forward_update_to_servers()` computes diff via `collect_commits_to_send()`, queues `ObjectUpdated` to each server. `forward_update_to_clients()` sends to clients with matching scope.

> `crates/groove/src/sync_manager.rs:1596-1617`

### From Server → Apply + Forward

`process_from_server()` applies via `apply_object_updated()`, forwards to clients in scope.

> `crates/groove/src/sync_manager.rs:1095`

### From Client → Role-Based Routing

| Client Role              | Behavior                                                |
| ------------------------ | ------------------------------------------------------- |
| `Peer` / `Admin`         | Apply directly, forward to servers + other clients      |
| `User` (no session)      | `SessionRequired` error                                 |
| `User` (catalogue write) | `CatalogueWriteDenied` error                            |
| `User` (row write)       | Queued as `PendingPermissionCheck` for ReBAC evaluation |

> `crates/groove/src/sync_manager.rs:1217-1480`

## QuerySettled and PersistenceAck

### QuerySettled: read-durability signal

`QuerySettled` is a tier-tagged signal that says: "this query has settled here at tier `T`."

One-hop flow (browser main thread -> worker):

1. Main sends `QuerySubscription { query_id, query, session, propagation }`.
2. Worker records `query_origin[query_id]` and queues `PendingQuerySubscription`.
3. Worker `QueryManager` compiles and settles the server-side graph.
4. If `propagation=full`, worker forwards upstream; if `local-only`, worker stops propagation at worker tier.
5. On first settle only, worker emits `QuerySettled { query_id, tier: Worker }`.
6. Main receives that payload, queues it in `pending_query_settled`, then `QueryManager::process()` moves it into `subscription.achieved_tiers`.
7. First delivery is held until `achieved_tiers` satisfies `durability_tier`; then the first callback is a full snapshot.
8. If local updates are configured as immediate, only later local write-driven updates bypass tier waiting. Initial delivery remains tier-gated.

> [`sync_manager/inbox.rs:279`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L279)
> [`sync_manager/inbox.rs:285`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L285)
> [`query_manager/server_queries.rs:243`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L243)
> [`query_manager/server_queries.rs:246`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L246)
> [`sync_manager/mod.rs:393`](../../crates/jazz-tools/src/sync_manager/mod.rs#L393)
> [`sync_manager/inbox.rs:113`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L113)
> [`query_manager/manager.rs:528`](../../crates/jazz-tools/src/query_manager/manager.rs#L528)
> [`query_manager/manager.rs:640`](../../crates/jazz-tools/src/query_manager/manager.rs#L640)
> [`query_manager/manager.rs:651`](../../crates/jazz-tools/src/query_manager/manager.rs#L651)

Key consequence: this is what makes `durability_tier` meaningful. Query state can settle and accumulate while delivery is gated, then unblock exactly when the required tier confirmation arrives.

### PersistenceAck: write-durability signal

`PersistenceAck` is a commit-level signal that says: "these commit IDs are durable at tier `T`."

One-hop flow (browser main thread -> worker):

1. Main sends `ObjectUpdated`.
2. Worker applies commits; for newly persisted commit IDs and if `my_tier` is set, worker emits `PersistenceAck` back to the sender.
3. Main receives `PersistenceAck`, stores tier state in storage, updates in-memory `commit.ack_state.confirmed_tiers`, and queues `(commit_id, tier)` for runtime consumers.
4. `RuntimeCore` drains received acks and resolves ack watchers whose requested tier is `<= acked_tier`.
5. Durable write watchers resolve at this step.

> [`sync_manager/inbox.rs:391`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L391)
> [`sync_manager/inbox.rs:395`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L395)
> [`sync_manager/inbox.rs:75`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L75)
> [`storage/opfs_btree.rs:431`](../../crates/jazz-tools/src/storage/opfs_btree.rs#L431)
> [`commit.rs:14`](../../crates/jazz-tools/src/commit.rs#L14)
> [`sync_manager/mod.rs:384`](../../crates/jazz-tools/src/sync_manager/mod.rs#L384)
> [`runtime_core.rs:389`](../../crates/jazz-tools/src/runtime_core.rs#L389)
> [`runtime_core.rs:752`](../../crates/jazz-tools/src/runtime_core.rs#L752)

Key consequence: delivery and durability are decoupled on purpose.

- `QuerySettled` gates first query result delivery semantics.
- `PersistenceAck` gates persisted-write completion semantics.
- Both use the same ordered tier lattice (`Worker < EdgeServer < GlobalServer`), but they answer different questions.

> [`sync_manager/types.rs:22`](../../crates/jazz-tools/src/sync_manager/types.rs#L22)

## Invariants

| Invariant                                                                   | Status                                                     |
| --------------------------------------------------------------------------- | ---------------------------------------------------------- |
| **Server completeness** (INV-S): All local objects synced to all servers    | Implemented — topological sort ensures parent-before-child |
| **Client no-leakage** (INV-C): Clients only receive in-scope updates        | Implemented — `is_in_scope()` check before sending         |
| **Metadata once** (INV-X): ObjectMetadata sent once per destination         | Implemented — tracked in `sent_metadata`                   |
| **Tip tracking** (INV-X): `sent_tips` accurately reflects destination state | Implemented                                                |

> `crates/groove/src/sync_manager.rs:1032-1080` (topological_sort)

## Design Decisions

1. **Query-based client scope**: Clients declare interest via queries; SyncManager enforces scope.
2. **Role-based auth** (replaces scope-based permissions): `ClientRole` (User/Admin/Peer) instead of per-object `Readable`/`ReadableAndWritable`.
3. **Pending permission checks**: User writes always queued for ReBAC evaluation — upper layer has policy flexibility.
4. **Metadata deduplication**: Object metadata sent exactly once per destination.
5. **Topological commit ordering**: Commits always sent parent-before-child for causal consistency.
6. **Client-chosen persistent IDs**: Clients generate and persist their own ClientId for session preservation across reconnects.
7. **Catalogue write protection**: User clients cannot write catalogue objects (`CatalogueWriteDenied`), avoiding the spec's "scope bypass" security gap.

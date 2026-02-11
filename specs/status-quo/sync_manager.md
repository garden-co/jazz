# Sync Manager — Status Quo

The Sync Manager coordinates data flow between nodes in the multi-tier topology (browser ↔ edge server ↔ core server). Its job is to ensure each node has the data it needs — no more, no less.

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

### PersistenceTier

```
Worker | EdgeServer | CoreServer
```

Used in PersistenceAck and QuerySettled for tier-aware durability.

> `crates/groove/src/sync_manager.rs:26-30`

## Message Protocol

### SyncPayload

| Variant                                     | Purpose                                      |
| ------------------------------------------- | -------------------------------------------- |
| `ObjectUpdated`                             | Object/branch commits (topologically sorted) |
| `ObjectTruncated`                           | Branch truncation notification               |
| `BlobRequest` / `BlobResponse`              | Blob transfer                                |
| `QuerySubscription` / `QueryUnsubscription` | Client query registration                    |
| `PersistenceAck`                            | Tier-level durability confirmation           |
| `QuerySettled`                              | Query results settled at tier                |
| `Error(SyncError)`                          | Error response                               |

> `crates/groove/src/sync_manager.rs:205-254`

Note: the spec originally called these `QueryRegistration`/`QueryUnregistration` — renamed to `QuerySubscription`/`QueryUnsubscription` in implementation.

### SyncError

| Variant                | Purpose                                |
| ---------------------- | -------------------------------------- |
| `PermissionDenied`     | Insufficient permission                |
| `BlobAccessDenied`     | Blob permission denied                 |
| `BlobNotFound`         | Blob not in storage                    |
| `SessionRequired`      | User client without session            |
| `CatalogueWriteDenied` | User client attempting catalogue write |

> `crates/groove/src/sync_manager.rs:169-190`

## Public API

### Connection Management

`add_server()`, `remove_server()`, `add_client()`, `remove_client()`, `set_client_role()`.

Adding a server triggers `queue_full_sync_to_server()` — pushes all existing objects.

**Design decision**: downward sync is always query-scoped. Clients receive data ONLY via query subscriptions — no "full dump" path.

> `crates/groove/src/sync_manager.rs:482-510`

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
| `send_query_subscription_to_servers()`   | Push queries upstream                           |
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

# Query/Sync Integration Plan

## Problem Summary

QueryManager.subscribe() creates local-only subscriptions. SyncManager.add_or_update_query() requires concrete `HashSet<(ObjectId, BranchName)>` scope. There's no bridge - clients can't subscribe to abstract queries and have matching objects sync automatically from servers.

## Constraints

- Single-tenant per server (queries global within scope)
- Push notification for new matching objects (real-time)
- Multi-tier hub/spoke (solution works at every layer)
- Offline critical (local queries must work disconnected)
- Client and server initialized with same schema

## Design: Server-Side QueryGraph

### Core Insight

Server runs the **same QueryGraph** as client. No simplified predicate evaluation - full query execution with reactive tracking of contributing ObjectIds. The QueryGraph already handles indices, filtering, joins - reuse it.

### Key Architectural Decision: Keep SyncManager Scope-Based

**SyncManager remains oblivious to QueryManager internals.** It only knows about scopes (`HashSet<(ObjectId, BranchName)>`), not Query structs or QueryGraphs.

The translation from Query → Scope happens in QueryManager:

```
Client sends: QuerySubscription { query_id, query, session }
           ↓
SyncManager: queues as pending, exposes via take_pending_query_subscriptions()
           ↓
QueryManager.process():
  - takes pending subscriptions from SyncManager
  - builds QueryGraph with client's session (for policy filtering)
  - settles graph against local indices
  - calls contributing_object_ids() to get scope
  - calls sync_manager.set_client_query_scope(client_id, query_id, scope)
  - sends ObjectUpdated for all objects in scope
           ↓
On local data changes:
  - QueryManager re-settles server-side QueryGraphs
  - Detects scope changes via contributing_object_ids()
  - Updates SyncManager scope
  - Sends ObjectUpdated for newly matching objects
```

This keeps:
- SyncManager simple and scope-based only
- All Query/Schema/Index knowledge in QueryManager
- Clean separation of concerns

### Extended SyncPayload

```rust
SyncPayload::QuerySubscription {
    query_id: QueryId,
    query: Query,           // Full query structure (serializable)
    session: Option<Session>,
}

SyncPayload::QueryUnsubscription {
    query_id: QueryId,
}
```

### Full Mechanism

```
┌─────────────────────────────────────────────────────────────────────┐
│                          CLIENT                                      │
│  1. subscribe_with_sync(query) → builds local QueryGraph            │
│  2. Sends QuerySubscription upstream                                │
│  3. Receives ObjectUpdated → indices update → QueryGraph reacts     │
└────────────────────────────────────┬────────────────────────────────┘
                                     │ QuerySubscription
                                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       MID-TIER SERVER                                │
│  1. SyncManager receives QuerySubscription, queues it               │
│  2. QueryManager.process() takes pending, builds QueryGraph         │
│  3. Settles graph, gets contributing_object_ids()                   │
│  4. Sets SyncManager scope for this query                           │
│  5. Forwards QuerySubscription upstream                             │
│  6. Receives ObjectUpdated from upstream → indices update           │
│  7. QueryGraph reacts → scope changes → forwards to client          │
└────────────────────────────────────┬────────────────────────────────┘
                                     │ QuerySubscription
                                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          HUB SERVER                                  │
│  1. SyncManager receives QuerySubscription, queues it               │
│  2. QueryManager.process() builds QueryGraph, settles               │
│  3. Gets contributing_object_ids(), sets scope                      │
│  4. Sends ObjectUpdated for all contributing objects                │
│  5. On local changes → QueryGraph reacts → sends new matches        │
└─────────────────────────────────────────────────────────────────────┘
```

### Reactive Scope Updates

When QueryGraph's contributing ObjectIds change:
- New ObjectId added → send ObjectUpdated downstream (if not already sent)
- ObjectId removed → (object stays synced, just no longer in this query's scope)

## Implementation Plan

### Part 1: Query Serialization ✅ COMPLETE

**Goal**: Query struct can be serialized for wire transmission.

Changes:
- Derive or implement Serialize/Deserialize for Query, Condition, JoinSpec, etc.
- Add QuerySubscription and QueryUnsubscription to SyncPayload

Files:
- `query_manager/query.rs` - Add serde derives
- `query_manager/types.rs` - Ensure Value, TableName, etc. are serializable
- `sync_manager.rs` - Add new SyncPayload variants

### Part 2: Contributing ObjectIds Tracking ✅ COMPLETE

**Goal**: QueryGraph can report which ObjectIds contribute to its result set.

Changes:
- Add method `QueryGraph::contributing_object_ids() -> HashSet<(ObjectId, BranchName)>`
- Track contributing IDs during graph settlement
- IDs come from IndexScanNode outputs that survive filtering

Files:
- `query_manager/graph.rs` - Add contributing_object_ids() method

### Part 3: Server-Side Query Subscription ✅ COMPLETE

**Goal**: Server builds QueryGraph when receiving QuerySubscription.

**Architecture**: SyncManager stays scope-based, QueryManager handles Query → Scope.

Changes to SyncManager:
- Add `pending_query_subscriptions: Vec<PendingQuerySubscription>` field
- On QuerySubscription receipt: queue to pending list (don't process)
- Add `take_pending_query_subscriptions()` method
- Add `set_client_query_scope(client_id, query_id, scope)` method

Changes to QueryManager:
- Add `server_subscriptions: HashMap<(ClientId, QueryId), ServerQuerySubscription>`
- In `process()`: take pending subscriptions, build QueryGraphs, set scopes
- On index updates: re-settle server QueryGraphs, detect scope changes

New struct in QueryManager:
```rust
struct ServerQuerySubscription {
    query: Query,
    graph: QueryGraph,
    session: Option<Session>,
    last_scope: HashSet<(ObjectId, BranchName)>,
}
```

Files:
- `sync_manager.rs` - Pending queue, take method, set_client_query_scope
- `query_manager/manager.rs` - ServerQuerySubscription tracking, process loop

**Tests**:
```rust
#[test]
fn server_builds_query_graph_on_subscription() {
    // Server has 3 users, 2 match "active = true"
    // Receive QuerySubscription
    // Verify server sends ObjectUpdated for the 2 matching users
}

#[test]
fn server_pushes_new_matches() {
    // Server already has query subscribed
    // Insert new matching user
    // Verify new user is pushed to subscribed client
}
```

### Part 4: Client subscribe_with_sync()

**Goal**: Client subscription automatically sends query upstream.

Changes:
- New method `QueryManager::subscribe_with_sync(query, session)`:
  1. Call existing `subscribe()` to build local QueryGraph
  2. Send QuerySubscription to SyncManager for upstream forwarding
  3. Return QuerySubscriptionId

Files:
- `query_manager/manager.rs` - Add subscribe_with_sync()

**Tests**:
```rust
#[test]
fn subscribe_with_sync_sends_upstream() {
    let sub_id = client_qm.subscribe_with_sync(query, None);
    client_qm.process();
    // Verify QuerySubscription in outbox to server
}
```

### Part 5: Multi-Tier Forwarding

**Goal**: Mid-tier servers forward queries upstream and relay objects downstream.

Changes:
- Server forwards received QuerySubscription to its upstream servers
- Server tracks which queries came from which clients
- When upstream sends ObjectUpdated, forward to clients with matching queries

Files:
- `sync_manager.rs` - Query forwarding logic (scope-based, existing pattern)
- `query_manager/manager.rs` - Forward QuerySubscription upstream

### Part 6: End-to-End Integration

**Tests**:
```rust
#[test]
fn e2e_client_receives_server_data_via_subscription() {
    // Server has existing data
    // Client subscribes (doesn't know ObjectIds)
    // Exchange messages
    // Client should have matching rows
}

#[test]
fn e2e_permissions_prevent_sync() {
    // Server has docs owned by Alice and Bob
    // Client subscribes as Alice
    // Client should ONLY receive Alice's docs
}
```

## Critical Files

- `crates/groove/src/sync_manager.rs` - Pending queue, scope management (NO Query knowledge)
- `crates/groove/src/query_manager/manager.rs` - Server subscriptions, Query → Scope translation
- `crates/groove/src/query_manager/graph.rs` - contributing_object_ids() method
- `crates/groove/src/query_manager/query.rs` - Serialization derives

## Design Decisions

1. **SyncManager stays scope-based**: No Query/QueryGraph knowledge in SyncManager
2. **QueryManager handles translation**: Query → Scope happens in QueryManager.process()
3. **Session in query**: Client's session MUST be included for correct permission evaluation
4. **No query dedup server-side**: Different clients may have different sessions/permissions

## Progress

- [x] Part 1: Query serialization
- [x] Part 2: contributing_object_ids()
- [x] Part 3: Server-side subscription handling
- [x] Part 4: Client subscribe_with_sync()
- [x] Part 5: Multi-tier forwarding
- [x] Part 6: E2E tests

## TODO (Future Work)

- **Reconnection**: When reconnect / dynamic server add-remove is implemented, client should re-send all active subscriptions to newly connected servers

## Known Shortcuts/Incomplete Items

1. **Error handling gap**: When query compilation fails server-side (e.g., invalid query, schema mismatch), the client is NOT notified. The server logs an error but the client's subscription silently fails. A proper error response payload should be added.

2. **Cleanup on disconnect**: When a client disconnects abruptly, `server_subscriptions` entries are not cleaned up. Should add `on_client_disconnect()` handler to QueryManager.

3. **Test coverage gaps**:
   - No tests for query compilation failure scenarios
   - No tests for scope shrinking (deletes/updates that remove matches from result set)
   - No tests for client disconnect cleanup
   - No tests for complex join queries with contributing IDs tracking

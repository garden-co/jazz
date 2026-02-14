# Query/Sync Integration — Status Quo

This is the bridge that makes query-driven sync work. The problem: a client says "I want all todos where done=false." The server needs to evaluate that query, find matching objects, send them to the client, and then keep watching for changes — sending new matches and retracting objects that no longer match.

Neither the [Query Manager](query_manager.md) nor the [Sync Manager](sync_manager.md) can do this alone. The Query Manager knows how to evaluate queries but doesn't know about network clients. The Sync Manager knows about clients and scopes but doesn't understand SQL. This integration layer connects them: queries produce scopes, scopes drive sync.

## Core Design

**SyncManager stays scope-based** — it only knows about scopes (`HashSet<(ObjectId, BranchName)>`), not Query structs or QueryGraphs. The translation from Query → Scope happens in QueryManager.

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

> `crates/groove/src/query_manager/manager.rs:2079-2213` (process_pending_query_subscriptions)
> `crates/groove/src/query_manager/graph.rs:181-218` (contributing_object_ids)

## Query Serialization

All relevant types (Query, Condition, Value, etc.) implement Serialize/Deserialize. SyncPayload includes `QuerySubscription` and `QueryUnsubscription` variants.

> `crates/groove/src/sync_manager.rs:204-254`

## Contributing ObjectIds

`QueryGraph::contributing_object_ids()` extracts ObjectIds from the output node, cross-references with IndexScanNode entries to pair each ID with its source branch.

> `crates/groove/src/query_manager/graph.rs:181-218`

## Server-Side Subscription Pipeline

1. SyncManager receives `QuerySubscription`, queues as `PendingQuerySubscription`
2. QueryManager takes pending, builds QueryGraph with client's session
3. Settles graph, extracts `contributing_object_ids()`
4. Calls `set_client_query_scope()` (triggers initial sync for matching objects)
5. Stores in `server_subscriptions` for reactive updates

> `crates/groove/src/sync_manager.rs:322-380` (PendingQuerySubscription, queue)
> `crates/groove/src/sync_manager.rs:605-669` (take/set methods)
> `crates/groove/src/query_manager/manager.rs:183-246` (ServerQuerySubscription)

## Client subscribe_with_sync()

`subscribe_with_sync(query, session, settled_tier)`:

1. Creates local subscription via `subscribe_with_session()`
2. Sends `QuerySubscription` to all connected servers
3. Returns `QuerySubscriptionId`

Also: `unsubscribe_with_sync()` for cleanup.

> `crates/groove/src/query_manager/manager.rs:1764-1800`

## Multi-Tier Forwarding

Server forwards received `QuerySubscription` to upstream servers. Tracks which clients originated each query via `query_origin: HashMap<QueryId, HashSet<ClientId>>` for relaying `QuerySettled` messages.

> `crates/groove/src/sync_manager.rs:389-391` (query_origin)
> `crates/groove/src/sync_manager.rs:671-704` (forwarding methods)
> `crates/groove/src/sync_manager.rs:1191-1204` (QuerySettled relay)

## Reconnect/Resubscribe Convergence

Active subscriptions are treated as desired state, not one-shot network events.

- `QueryManager::add_server()` calls `SyncManager::add_server()` and then replays all active local and downstream query subscriptions to the new upstream.
- Replay behavior is deterministic across connection timing: if a subscription is active when upstream reconnects, it is replayed; if it was unsubscribed, it is not replayed.
- This gives anti-entropy for query forwarding without requiring subscribe/connect timing coordination.

> `crates/groove/src/query_manager/subscriptions.rs:190-240` (add_server + replay_active_query_subscriptions_to_server)
> `crates/groove/src/query_manager/manager_tests.rs:5116-5160` (add_server_replays_existing_local_query_subscriptions)
> `crates/groove/src/runtime_core.rs:1539-1594` (replay on reconnect + no replay after unsubscribe)

## Lazy Schema Activation (Server Mode)

Servers don't know schemas in advance — they discover them from clients via catalogue sync. When the first client connects with schema v1, the server receives the schema object and lazily activates it. This means servers need no deployment coordination when clients ship new schema versions.

1. `known_schemas` synced from SchemaManager to QueryManager via `set_known_schemas()`
2. `find_schema_by_short_hash()` matches incoming branch names to full hashes
3. Row objects on unknown branches trigger lazy branch activation
4. Table schema lookup falls through: current → live (context) → known_schemas

> `crates/groove/src/query_manager/manager.rs:261-265` (known_schemas field)
> `crates/groove/src/query_manager/manager.rs:1853-1867` (find_schema_by_short_hash)

## Reactive Scope Updates

This is the "keep watching" part. After the initial sync, the server's query graph keeps running. When data changes (new inserts, updates from other clients), the graph re-settles and the scope may change:

- New ObjectId added to scope → send ObjectUpdated downstream (client gets new data)
- ObjectId removed from scope → stays synced (data already sent), just no longer in this query's scope (client won't get future updates to it)

> `crates/groove/src/query_manager/manager.rs:2239-2320` (settle_server_subscriptions)

## Design Decisions

1. **SyncManager stays scope-based**: No Query/QueryGraph knowledge in SyncManager
2. **QueryManager handles translation**: Query → Scope in `process()`
3. **Session in query**: Client's session included for correct permission evaluation
4. **No query dedup**: Different clients may have different sessions/permissions
5. **Schema context in requests**: All requests include `QuerySchemaContext` for server-side execution

## Test Coverage

- `e2e_two_clients_query_subscriptions_through_server`: Client A (alice) and B (bob) subscribe to documents, each receives only their own (policy filtering works)
- `e2e_two_clients_server_schema_sync`: Server bootstraps from empty state, lazy activation works

> `crates/groove/src/schema_manager/integration_tests.rs:1668-2202`

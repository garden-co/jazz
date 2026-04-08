# Query/Sync Integration — Status Quo

This is the bridge that makes query-driven sync work. The problem: a client says "I want all todos where done=false." The server needs to evaluate that query, find matching objects, send them to the client, and then keep watching for changes — sending new matches and retracting objects that no longer match.

Neither the [Query Manager](query_manager.md) nor the [Sync Manager](sync_manager.md) can do this alone. The Query Manager knows how to evaluate queries but doesn't know about network clients. The Sync Manager knows about clients and scopes but doesn't understand SQL. This integration layer connects them: queries produce scopes, scopes drive sync.

## Core Design

**SyncManager stays scope-based** — it only knows about scopes (`HashSet<(ObjectId, BranchName)>`), not Query structs or QueryGraphs. The translation from Query → Scope happens in QueryManager.

```
Client sends: QuerySubscription { query_id, query, session, propagation }
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

`subscribe_with_sync_and_propagation(query, session, durability_tier, propagation)`:

1. Creates local subscription via `subscribe_with_session()`
2. Sends `QuerySubscription` to connected servers based on propagation mode
3. Returns `QuerySubscriptionId`

Also: `unsubscribe_with_sync()` for cleanup.

Propagation behavior:

- `full` (default): forward subscription and unsubscription upstream; replay on reconnect.
- `local-only`: do not forward past the local durability boundary. In browser main->worker topology this still reaches worker (OPFS tier), but worker will not forward to edge/global.

> [`query_manager/subscriptions.rs:26`](../../crates/jazz-tools/src/query_manager/subscriptions.rs#L26)
> [`query_manager/subscriptions.rs:205`](../../crates/jazz-tools/src/query_manager/subscriptions.rs#L205)
> [`sync_manager/mod.rs:128`](../../crates/jazz-tools/src/sync_manager/mod.rs#L128)

## Multi-Tier Forwarding

Server forwards received `QuerySubscription` to upstream servers only when `propagation == full`. It tracks which clients originated each query via `query_origin: HashMap<QueryId, HashSet<ClientId>>` for relaying `QuerySettled` messages.

> `crates/groove/src/sync_manager.rs:389-391` (query_origin)
> `crates/groove/src/sync_manager.rs:671-704` (forwarding methods)
> `crates/groove/src/sync_manager.rs:1191-1204` (QuerySettled relay)

## QuerySettled Integration (Detailed)

`QuerySettled` is the integration point between upstream query execution and downstream first-delivery guarantees.

End-to-end path:

1. Local `subscribe_with_sync_and_propagation(query, session, durability_tier, propagation)` creates a local subscription and conditionally forwards `QuerySubscription` upstream.
2. Upstream/server `QueryManager` compiles + settles a server-side graph and computes scope.
3. When the server emits sync updates to a client connection, each outgoing `SyncUpdate` gets a monotonically increasing connection-local `seq`.
4. If the payload is `QuerySettled`, the transport stamps it with `through_seq = seq - 1`, meaning "this settlement is only valid after the client has applied every earlier sync update on this connection."
5. Any intermediate sync node relays that payload to original downstream clients via `query_origin`, preserving the watermark.
6. Receiver stores `PendingQuerySettled { server_id, query_id, tier, through_seq }` instead of immediately raising the query's achieved tier.
7. In local `RuntimeCore::immediate_tick()`, pending settlements are released only after the runtime has applied all sync updates through `through_seq` for that upstream server.
8. Only then does `QueryManager` mark `achieved_tiers >= tier`; if the subscription was already waiting on that tier, first delivery can proceed in the same tick.
9. With `local_updates = Immediate`, local write deltas can bypass tier waiting only after that first delivery (`settled_once = true`). Initial delivery never bypasses tier gating.

> [`server/mod.rs`](../../crates/jazz-tools/src/server/mod.rs)
> [`routes.rs`](../../crates/jazz-tools/src/routes.rs)
> [`sync_manager/inbox.rs`](../../crates/jazz-tools/src/sync_manager/inbox.rs)
> [`sync_manager/mod.rs`](../../crates/jazz-tools/src/sync_manager/mod.rs)
> [`runtime_core/ticks.rs`](../../crates/jazz-tools/src/runtime_core/ticks.rs)
> [`query_manager/manager.rs`](../../crates/jazz-tools/src/query_manager/manager.rs)
> [`runtime/sync-transport.ts`](../../packages/jazz-tools/src/runtime/sync-transport.ts)

Why this ordering matters:

- `ObjectUpdated` may arrive in the same batch as `QuerySettled`.
- `QuerySettled` can arrive before some earlier `ObjectUpdated` frames on the wire; `through_seq` prevents first delivery from treating that settlement as complete too early.
- Once the watermark is satisfied, first delivery can still unblock in the same tick as the release.
- If tier is not satisfied, query state still settles locally; only delivery is deferred.
- `local_updates = Immediate` changes post-initial behavior only: later local writes can still notify immediately while waiting on higher-tier confirmation.

## PersistenceAck Integration (Detailed)

`PersistenceAck` is the durability side of the same sync fabric. It does not gate query callbacks; it gates persisted write completion.

End-to-end path:

1. Durable write APIs register an ack watcher keyed by commit ID and requested tier.
2. Commit is synced upstream via `ObjectUpdated`.
3. Receiver with `my_tier` set applies the commit and emits `PersistenceAck` for newly persisted commit IDs.
4. Ack receiver stores tier to storage and in-memory commit ack state, then queues it for runtime.
5. Runtime consumes received acks and resolves watchers where `acked_tier >= requested_tier`.

> [`runtime_core.rs:752`](../../crates/jazz-tools/src/runtime_core.rs#L752)
> [`runtime_core.rs:776`](../../crates/jazz-tools/src/runtime_core.rs#L776)
> [`runtime_core.rs:824`](../../crates/jazz-tools/src/runtime_core.rs#L824)
> [`sync_manager/inbox.rs:391`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L391)
> [`sync_manager/inbox.rs:395`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L395)
> [`sync_manager/inbox.rs:75`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L75)
> [`sync_manager/inbox.rs:83`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L83)
> [`runtime_core.rs:389`](../../crates/jazz-tools/src/runtime_core.rs#L389)
> [`runtime_core.rs:399`](../../crates/jazz-tools/src/runtime_core.rs#L399)

Relay behavior for multi-hop durability:

- Upstream/intermediate nodes keep `commit_interest` so incoming acks can be fanned out to downstream clients that originated the commit.

> [`sync_manager/mod.rs:52`](../../crates/jazz-tools/src/sync_manager/mod.rs#L52)
> [`sync_manager/inbox.rs:383`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L383)
> [`sync_manager/inbox.rs:95`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L95)

Practical distinction:

- `QuerySettled`: "is the query result ready at tier T?"
- `PersistenceAck`: "is this commit durable at tier T?"

## Reconnect/Resubscribe Convergence

Active subscriptions are treated as desired state, not one-shot network events.

- `QueryManager::add_server()` calls `SyncManager::add_server()` and then replays active subscriptions that are `propagation=full` to the new upstream.
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

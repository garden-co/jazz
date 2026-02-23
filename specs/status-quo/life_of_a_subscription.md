# Life of a Query Subscription (Main Thread -> Worker -> OPFS) — Status Quo

This walkthrough traces one sync hop in the browser stack:

1. Main-thread runtime (in-memory)
2. Dedicated worker runtime (persistent OPFS)

It covers both:

- one-shot query calls (`db.all(app.todos.where(...))`)
- live query subscriptions (`db.subscribeAll(app.todos.where(...), ...)`)

## Scope and Example Calls

At the app level, these are the two important entry points:

```ts
// one-shot read
const rows = await db.all(app.todos.where({ done: false }));

// live read
const unsubscribe = db.subscribeAll(app.todos.where({ done: false }), ({ all }) => {
  console.log(all.length);
});
```

> [`src/docs-snippets.ts:9`](../../examples/docs/todo-client-localfirst-ts/src/docs-snippets.ts#L9)
> [`src/docs-snippets.ts:15`](../../examples/docs/todo-client-localfirst-ts/src/docs-snippets.ts#L15)

Schema starts from TS DSL:

```ts
table("todos", {
  title: col.string(),
  done: col.boolean(),
  project: col.ref("projects"),
});
```

> [`basic/current.ts:7`](../../packages/jazz-tools/tests/ts-dsl/fixtures/basic/current.ts#L7)

## 1. From TS DSL to Query JSON

The schema DSL is side-effect collected (`table(...)` pushes built columns into collected tables), then codegen emits typed query builders.

> [`src/dsl.ts:208`](../../packages/jazz-tools/src/dsl.ts#L208)
> [`src/cli.ts:50`](../../packages/jazz-tools/src/cli.ts#L50)
> [`src/cli.ts:70`](../../packages/jazz-tools/src/cli.ts#L70)
> [`codegen/query-builder-generator.ts:291`](../../packages/jazz-tools/src/codegen/query-builder-generator.ts#L291)

Generated builder output is JSON:

```ts
_build(): string {
  return JSON.stringify({
    table: this._table,
    conditions: this._conditions,
    includes: this._includes,
    orderBy: this._orderBys,
    limit: this._limitVal,
    offset: this._offsetVal,
  });
}
```

> [`basic/app.ts:264`](../../packages/jazz-tools/tests/ts-dsl/fixtures/basic/app.ts#L264)

`Db` then translates this builder JSON to runtime query JSON (`relation_ir`, normalized conditions/order/limit/offset, etc):

> [`runtime/db.ts:531`](../../packages/jazz-tools/src/runtime/db.ts#L531)
> [`runtime/db.ts:582`](../../packages/jazz-tools/src/runtime/db.ts#L582)
> [`runtime/query-adapter.ts:556`](../../packages/jazz-tools/src/runtime/query-adapter.ts#L556)

## 2. Browser Setup: Main Runtime + Worker Runtime

In browser mode, `createDb()` uses worker-backed mode:

- main thread: in-memory `WasmRuntime`
- dedicated worker: persistent `WasmRuntime.openPersistent(...)`
- bridge: forwards sync envelopes main <-> worker via `postMessage`

> [`runtime/db.ts:672`](../../packages/jazz-tools/src/runtime/db.ts#L672)
> [`runtime/db.ts:268`](../../packages/jazz-tools/src/runtime/db.ts#L268)
> [`runtime/db.ts:315`](../../packages/jazz-tools/src/runtime/db.ts#L315)
> [`runtime/worker-bridge.ts:44`](../../packages/jazz-tools/src/runtime/worker-bridge.ts#L44)
> [`worker/groove-worker.ts:107`](../../packages/jazz-tools/src/worker/groove-worker.ts#L107)

Important bridge behavior: main runtime is given a logical upstream server (`runtime.addServer()`), and that upstream is implemented by the worker.

> [`runtime/worker-bridge.ts:69`](../../packages/jazz-tools/src/runtime/worker-bridge.ts#L69)

## 3. Main-Thread Internals First (Assume Data Already In Memory)

This section focuses on the local runtime path before forwarding to worker.

Assumption for this section: the main-thread object/index state already has relevant row data (for example from earlier inserts or from prior sync), so query settlement can run immediately.

### 3.1 One-shot path: `db.all(app.todos.where(...))`

Call chain:

1. `Db.all()` builds and translates query
2. `JazzClient.query()` -> `queryInternal()`
3. `runtime.query(...)` (WASM binding)
4. Rust `RuntimeCore::query(...)` creates a temporary subscription and waits for first callback
5. `immediate_tick()` / `QueryManager::process()` compile then settle that subscription graph
6. first snapshot resolves the query future, then auto-unsubscribes

> [`runtime/db.ts:531`](../../packages/jazz-tools/src/runtime/db.ts#L531)
> [`runtime/client.ts:480`](../../packages/jazz-tools/src/runtime/client.ts#L480)
> [`src/runtime.rs:436`](../../crates/jazz-wasm/src/runtime.rs#L436)
> [`src/runtime_core.rs:632`](../../crates/jazz-tools/src/runtime_core.rs#L632)
> [`src/runtime_core.rs:641`](../../crates/jazz-tools/src/runtime_core.rs#L641)
> [`src/runtime_core.rs:346`](../../crates/jazz-tools/src/runtime_core.rs#L346)
> [`src/runtime_core.rs:384`](../../crates/jazz-tools/src/runtime_core.rs#L384)

### 3.2 Live path: `db.subscribeAll(app.todos.where(...), cb)`

Call chain:

1. `Db.subscribeAll()` -> `client.subscribe(...)`
2. `JazzClient.subscribeInternal(...)` -> `runtime.subscribe(...)`
3. Rust `RuntimeCore::subscribe_with_settled_tier(...)`
4. first `process()` pass compiles and settles; callback then remains registered for incremental updates

> [`runtime/db.ts:582`](../../packages/jazz-tools/src/runtime/db.ts#L582)
> [`runtime/client.ts:573`](../../packages/jazz-tools/src/runtime/client.ts#L573)
> [`src/runtime.rs:642`](../../crates/jazz-wasm/src/runtime.rs#L642)
> [`src/runtime_core.rs:556`](../../crates/jazz-tools/src/runtime_core.rs#L556)

### 3.3 What QueryManager does during `process()` on main thread

`QueryManager::new(...)` subscribes to the global object-update stream in `ObjectManager`, so incoming object changes can mark query graphs dirty.

During `process()`, compilation happens before settlement:

1. consume sync inbox and object updates
2. process pending query subscriptions/unsubscriptions (new subscriptions are compiled here)
3. settle policy checks
4. settle local subscription graphs
5. emit `update_outbox`

This ordering is what makes "new subscription + already-present data in same tick" work.

> [`query_manager/manager.rs:265`](../../crates/jazz-tools/src/query_manager/manager.rs#L265)
> [`query_manager/manager.rs:492`](../../crates/jazz-tools/src/query_manager/manager.rs#L492)
> [`query_manager/manager.rs:509`](../../crates/jazz-tools/src/query_manager/manager.rs#L509)
> [`query_manager/manager.rs:511`](../../crates/jazz-tools/src/query_manager/manager.rs#L511)
> [`query_manager/manager.rs:537`](../../crates/jazz-tools/src/query_manager/manager.rs#L537)

### 3.4 QueryGraph compilation (focus: graph shape)

After a `Query` reaches Rust, QueryManager compiles it through one entrypoint:

`compile_graph(...) -> QueryGraph::try_compile_with_schema_context(...)`

Core compile steps:

1. resolve branches (explicit query branches, or all live schema branches)
2. validate relation tables (`ensure_relation_tables_exist`)
3. lower `relation_ir` to `ExecutionQueryPlan`
4. build a `QueryGraph` from that plan

> [`query_manager/manager.rs:362`](../../crates/jazz-tools/src/query_manager/manager.rs#L362)
> [`query_manager/graph.rs:863`](../../crates/jazz-tools/src/query_manager/graph.rs#L863)
> [`query_manager/graph.rs:878`](../../crates/jazz-tools/src/query_manager/graph.rs#L878)
> [`query_manager/graph.rs:319`](../../crates/jazz-tools/src/query_manager/graph.rs#L319)
> [`query_manager/relation_ir_query_plan.rs:668`](../../crates/jazz-tools/src/query_manager/relation_ir_query_plan.rs#L668)

Compilation emits a mostly fixed pipeline:

```text
IndexScan* -> (Union?) -> Materialize -> (PolicyFilter?)
          -> (ArraySubquery*) -> (Filter?) -> (Sort?) -> (LimitOffset?)
          -> (Project?) -> (RecursiveRelation?) -> Output
```

Notes:

- index scans are per `(branch, disjunct)` pair
- join-shaped plans use `compile_join_plan(...)` but still terminate in `Output`
- graph metadata tracks dependency tables (`index_scan_nodes`, `array_subquery_tables`, `policy_filter_tables`, `recursive_relation_tables`) for later dirty marking

> [`query_manager/graph.rs:587`](../../crates/jazz-tools/src/query_manager/graph.rs#L587)
> [`query_manager/graph.rs:676`](../../crates/jazz-tools/src/query_manager/graph.rs#L676)
> [`query_manager/graph.rs:708`](../../crates/jazz-tools/src/query_manager/graph.rs#L708)
> [`query_manager/graph.rs:791`](../../crates/jazz-tools/src/query_manager/graph.rs#L791)
> [`query_manager/graph.rs:1174`](../../crates/jazz-tools/src/query_manager/graph.rs#L1174)

### 3.5 QueryGraph settling (focus: execution and deltas)

Settlement is a dirty-node execution pass over that compiled graph.

1. object/index changes mark graph nodes dirty (`mark_dirty_for_table`, `mark_row_updated`, `mark_row_deleted`)
2. `QueryManager` calls `subscription.graph.settle(storage, row_loader)`
3. `settle()` topologically orders dirty nodes and evaluates each node kind
4. `Materialize` is the content boundary: loads row bytes, detects deleted/new/updated tuples
5. output tuple deltas are converted to row deltas, dirty flags are cleared

> [`query_manager/graph.rs:1498`](../../crates/jazz-tools/src/query_manager/graph.rs#L1498)
> [`query_manager/graph.rs:1613`](../../crates/jazz-tools/src/query_manager/graph.rs#L1613)
> [`query_manager/graph.rs:1638`](../../crates/jazz-tools/src/query_manager/graph.rs#L1638)
> [`query_manager/manager.rs:630`](../../crates/jazz-tools/src/query_manager/manager.rs#L630)
> [`query_manager/graph.rs:1676`](../../crates/jazz-tools/src/query_manager/graph.rs#L1676)
> [`query_manager/graph.rs:1712`](../../crates/jazz-tools/src/query_manager/graph.rs#L1712)
> [`query_manager/graph.rs:1897`](../../crates/jazz-tools/src/query_manager/graph.rs#L1897)
> [`query_manager/graph.rs:2080`](../../crates/jazz-tools/src/query_manager/graph.rs#L2080)

Main-thread `row_loader` behavior during settle:

- loads candidate row objects across the subscription branches
- picks LWW commit by timestamp
- drops empty-content hard-delete tombstones
- applies lens transform when source branch schema is older

> [`query_manager/manager.rs:566`](../../crates/jazz-tools/src/query_manager/manager.rs#L566)
> [`query_manager/manager.rs:573`](../../crates/jazz-tools/src/query_manager/manager.rs#L573)
> [`query_manager/manager.rs:605`](../../crates/jazz-tools/src/query_manager/manager.rs#L605)
> [`query_manager/manager.rs:609`](../../crates/jazz-tools/src/query_manager/manager.rs#L609)

Delivery semantics after settle:

- if required settled tier is not achieved, graph state updates but delivery is held
- first delivery is a full snapshot via `current_result_as_delta()`
- later deliveries are incremental deltas

> [`query_manager/manager.rs:640`](../../crates/jazz-tools/src/query_manager/manager.rs#L640)
> [`query_manager/manager.rs:651`](../../crates/jazz-tools/src/query_manager/manager.rs#L651)
> [`query_manager/manager.rs:667`](../../crates/jazz-tools/src/query_manager/manager.rs#L667)
> [`query_manager/graph.rs:2121`](../../crates/jazz-tools/src/query_manager/graph.rs#L2121)

## 4. Main Thread -> Worker: Sync Forwarding Boundary

At the hop boundary, main thread does **not** send a compiled graph; it sends a sync query subscription payload:

```rust
self.sync_manager.send_query_subscription_to_servers(query_id, sync_query, session);
```

> [`query_manager/subscriptions.rs:175`](../../crates/jazz-tools/src/query_manager/subscriptions.rs#L175)

Wire shape:

```rust
SyncPayload::QuerySubscription { query_id, query, session }
```

> [`sync_manager/types.rs:225`](../../crates/jazz-tools/src/sync_manager/types.rs#L225)

So across this hop, the important state is: `query_id + query + session`.

## 5. Worker Thread: Same Core Pattern, but Persistent

Worker receives `SyncPayload::QuerySubscription`, queues it, then runs the same compile/settle machinery as main thread, but for server-side scope tracking.

In `process_pending_query_subscriptions()`:

```rust
let graph = Self::compile_graph(&sub.query, &schema_for_compile, sub.session.clone(), &self.schema_context);
let _delta = graph.settle(storage_ref, row_loader);
let scope = graph.contributing_object_ids();
self.sync_manager.set_client_query_scope(sub.client_id, sub.query_id, scope.clone(), sub.session.clone());
```

> [`sync_manager/inbox.rs:279`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L279)
> [`query_manager/server_queries.rs:55`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L55)
> [`query_manager/server_queries.rs:131`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L131)
> [`query_manager/server_queries.rs:134`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L134)

Then worker stores this as a `server_subscription` and re-settles it on later object updates. If the contributing scope changes, scope is updated and sync emits the corresponding object visibility changes.

```rust
let _delta = sub.graph.settle(storage, row_loader);
let new_scope = sub.graph.contributing_object_ids();
if new_scope != sub.last_scope { ... set_client_query_scope(...) ... }
```

> [`query_manager/server_queries.rs:150`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L150)
> [`query_manager/server_queries.rs:244`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L244)
> [`query_manager/server_queries.rs:253`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L253)
> [`query_manager/server_queries.rs:267`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L267)

If worker-side compile fails, it emits `QuerySubscriptionRejected` to the client.

> [`query_manager/server_queries.rs:62`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L62)

## 6. Worker Persistence Layer (OPFS)

Worker startup opens persistent runtime:

```ts
runtime = await wasmModule.WasmRuntime.openPersistent(..., dbName, "worker");
```

> [`worker/groove-worker.ts:107`](../../packages/jazz-tools/src/worker/groove-worker.ts#L107)

Rust side creates `OpfsBTreeStorage::open_opfs(...)`:

> [`src/runtime.rs:829`](../../crates/jazz-wasm/src/runtime.rs#L829)
> [`src/runtime.rs:887`](../../crates/jazz-wasm/src/runtime.rs#L887)
> [`storage/opfs_btree.rs:119`](../../crates/jazz-tools/src/storage/opfs_btree.rs#L119)

Commit/object writes go through ObjectManager -> Storage:

> [`object_manager/mod.rs:516`](../../crates/jazz-tools/src/object_manager/mod.rs#L516)
> [`object_manager/mod.rs:539`](../../crates/jazz-tools/src/object_manager/mod.rs#L539)
> [`object_manager/mod.rs:582`](../../crates/jazz-tools/src/object_manager/mod.rs#L582)

Durability barrier runs at `batched_tick()`:

> [`src/runtime_core.rs:468`](../../crates/jazz-tools/src/runtime_core.rs#L468)
> [`src/runtime_core.rs:471`](../../crates/jazz-tools/src/runtime_core.rs#L471)
> [`storage/opfs_btree.rs:563`](../../crates/jazz-tools/src/storage/opfs_btree.rs#L563)
> [`storage/opfs_btree.rs:566`](../../crates/jazz-tools/src/storage/opfs_btree.rs#L566)

And OPFS access is dedicated-worker-only:

> [`src/file.rs:268`](../../crates/opfs-btree/src/file.rs#L268)
> [`src/file.rs:270`](../../crates/opfs-btree/src/file.rs#L270)

## 7. Worker -> Main Return Path and Callback Delivery

Worker emits client-destination sync payloads back to main:

> [`worker/groove-worker.ts:127`](../../packages/jazz-tools/src/worker/groove-worker.ts#L127)
> [`worker/groove-worker.ts:130`](../../packages/jazz-tools/src/worker/groove-worker.ts#L130)
> [`worker/groove-worker.ts:56`](../../packages/jazz-tools/src/worker/groove-worker.ts#L56)

Main bridge feeds them into main runtime:

> [`runtime/worker-bridge.ts:49`](../../packages/jazz-tools/src/runtime/worker-bridge.ts#L49)
> [`runtime/worker-bridge.ts:54`](../../packages/jazz-tools/src/runtime/worker-bridge.ts#L54)

Main runtime then settles local subscriptions and invokes JS callbacks:

> [`src/runtime_core.rs:343`](../../crates/jazz-tools/src/runtime_core.rs#L343)
> [`src/runtime_core.rs:364`](../../crates/jazz-tools/src/runtime_core.rs#L364)
> [`src/runtime.rs:665`](../../crates/jazz-wasm/src/runtime.rs#L665)

For `db.all(...)`, that first snapshot resolves the one-shot future and unsubscribes.
For `db.subscribeAll(...)`, callback remains active and receives later deltas.

## 8. Why `db.all(...)` and `db.subscribeAll(...)` Feel Similar

Both are implemented on top of the same reactive machinery:

- one-shot query is "subscribe once, resolve on first settled snapshot, then unsubscribe"
- live query is "subscribe and keep subscription state"

That shared path is why both participate in the same tier-settlement, lens-transform, and sync-forwarding behavior.

> [`src/runtime_core.rs:632`](../../crates/jazz-tools/src/runtime_core.rs#L632)
> [`src/runtime_core.rs:656`](../../crates/jazz-tools/src/runtime_core.rs#L656)
> [`src/runtime_core.rs:570`](../../crates/jazz-tools/src/runtime_core.rs#L570)
> [`src/runtime_core.rs:597`](../../crates/jazz-tools/src/runtime_core.rs#L597)

## 9. Minimal Sequence (One Hop)

```text
app.todos.where(...) / db.all(...) / db.subscribeAll(...)
  -> main QueryManager subscription created and settled locally
  -> main SyncManager emits QuerySubscription
  -> WorkerBridge postMessage(sync)
  -> worker SyncManager inbox queues pending query subscription
  -> worker QueryManager compiles + settles + computes scope
  -> worker queues ObjectUpdated back to main client
  -> main receives sync payload and updates local subscription result
  -> callback/future resolves on main thread
```

## 10. Reference Test

Browser integration test that exercises this bridge path with real worker + OPFS:

> [`browser/worker-bridge.test.ts:289`](../../packages/jazz-tools/tests/browser/worker-bridge.test.ts#L289)
> [`browser/worker-bridge.test.ts:294`](../../packages/jazz-tools/tests/browser/worker-bridge.test.ts#L294)
> [`browser/worker-bridge.test.ts:258`](../../packages/jazz-tools/tests/browser/worker-bridge.test.ts#L258)

## 11. Read Policy Checks in Compilation and Settlement

Read policy is part of the same query graph lifecycle, not a separate pass.

Connection flow:

1. compile inserts `PolicyFilterNode` into the graph when there is a session and a table `SELECT USING` policy
2. settle evaluates that node as part of normal dirty-node execution
3. node output controls which rows continue downstream to `Output`, so policy filtering directly shapes query deltas/callbacks
4. if policy depends on other tables (`INHERITS`/`EXISTS`), those tables are tracked; updates there mark policy nodes dirty and force re-evaluation

> [`query_manager/graph.rs:435`](../../crates/jazz-tools/src/query_manager/graph.rs#L435)
> [`query_manager/graph.rs:686`](../../crates/jazz-tools/src/query_manager/graph.rs#L686)
> [`query_manager/graph.rs:1234`](../../crates/jazz-tools/src/query_manager/graph.rs#L1234)
> [`query_manager/graph.rs:1321`](../../crates/jazz-tools/src/query_manager/graph.rs#L1321)
> [`query_manager/graph.rs:1945`](../../crates/jazz-tools/src/query_manager/graph.rs#L1945)
> [`query_manager/graph.rs:1540`](../../crates/jazz-tools/src/query_manager/graph.rs#L1540)
> [`graph_nodes/policy_filter.rs:118`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L118)
> [`graph_nodes/policy_filter.rs:122`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L122)

What `PolicyFilterNode` does internally:

1. keeps `current_tuples` (the currently visible rows after policy)
2. on `added`, evaluates policy and only forwards passing tuples
3. on `removed`, removes from visible set if present
4. on `updated`, computes visibility transition:
   `true->true` update, `true->false` remove, `false->true` add, `false->false` no output

> [`graph_nodes/policy_filter.rs:31`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L31)
> [`graph_nodes/policy_filter.rs:621`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L621)
> [`graph_nodes/policy_filter.rs:681`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L681)

It has two evaluation modes, which is how it connects to settle behavior:

- non-context mode (`evaluate`) handles simple policy expressions and fails closed for contextual clauses (`EXISTS`, `EXISTS REL`, non-null `INHERITS` FK)
- context mode (`process_with_context`) is used when `has_inherits()` is true, and can load related rows / run contextual checks

> [`graph_nodes/policy_filter.rs:467`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L467)
> [`graph_nodes/policy_filter.rs:489`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L489)
> [`graph_nodes/policy_filter.rs:502`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L502)
> [`graph_nodes/policy_filter.rs:122`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L122)

In context mode, `EXISTS REL` can build and settle a temporary `PolicyGraph` for the bound relation expression, then use that boolean result in the parent policy decision.

> [`graph_nodes/policy_filter.rs:430`](../../crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs#L430)
> [`query_manager/policy_graph.rs:201`](../../crates/jazz-tools/src/query_manager/policy_graph.rs#L201)

Example schema/query (from tests):

```rust
TablePolicies::new().with_select(
  PolicyExpr::or(vec![
    PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
    PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]),
  ]),
);

let query = qm.query("documents").build();
let sub_id = qm.subscribe_with_session(query, Some(alice_session), None)?;
```

> [`query_manager/manager_tests.rs:4655`](../../crates/jazz-tools/src/query_manager/manager_tests.rs#L4655)
> [`query_manager/manager_tests.rs:4665`](../../crates/jazz-tools/src/query_manager/manager_tests.rs#L4665)
> [`query_manager/manager_tests.rs:4748`](../../crates/jazz-tools/src/query_manager/manager_tests.rs#L4748)

## 12. Write Requests and Ad-Hoc Policy Query Graphs

Write policy checks are connected to the same event loop, but they run as ad-hoc one-shot graphs before writes are applied.

Connection flow:

1. incoming row write/delete is queued as `PendingPermissionCheck`
2. `QueryManager::process()` picks checks up before normal subscription settlement
3. `evaluate_simple_parts(...)` handles fast-path policy clauses synchronously
4. unresolved complex clauses (`INHERITS`/`EXISTS`/`EXISTS REL`) are compiled into ad-hoc `PolicyGraph`s
5. policy graphs settle; pass => `approve_permission_check` applies payload, fail => `reject_permission_check` sends `PermissionDenied`
6. approved write emits object updates, which then dirty and re-settle regular query subscription graphs

> [`sync_manager/inbox.rs:188`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L188)
> [`sync_manager/inbox.rs:213`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L213)
> [`sync_manager/inbox.rs:265`](../../crates/jazz-tools/src/sync_manager/inbox.rs#L265)
> [`query_manager/manager.rs:516`](../../crates/jazz-tools/src/query_manager/manager.rs#L516)
> [`query_manager/server_queries.rs:295`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L295)
> [`query_manager/policy.rs:984`](../../crates/jazz-tools/src/query_manager/policy.rs#L984)
> [`query_manager/policy.rs:1034`](../../crates/jazz-tools/src/query_manager/policy.rs#L1034)
> [`query_manager/server_queries.rs:546`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L546)
> [`query_manager/server_queries.rs:646`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L646)
> [`sync_manager/permissions.rs:19`](../../crates/jazz-tools/src/sync_manager/permissions.rs#L19)
> [`sync_manager/permissions.rs:31`](../../crates/jazz-tools/src/sync_manager/permissions.rs#L31)
> [`query_manager/manager.rs:910`](../../crates/jazz-tools/src/query_manager/manager.rs#L910)

`PolicyGraph` shape is intentionally minimal and one-shot:

```text
IndexScan -> Materialize -> PolicyFilter -> ExistsOutput
```

> [`query_manager/policy_graph.rs:40`](../../crates/jazz-tools/src/query_manager/policy_graph.rs#L40)
> [`query_manager/policy_graph.rs:87`](../../crates/jazz-tools/src/query_manager/policy_graph.rs#L87)
> [`query_manager/policy_graph.rs:233`](../../crates/jazz-tools/src/query_manager/policy_graph.rs#L233)

`UPDATE` is explicitly two-phase (`USING` on old row, `WITH CHECK` on new row), and complex clauses from either side can spawn policy graphs.

> [`query_manager/server_queries.rs:414`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L414)
> [`query_manager/server_queries.rs:439`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L439)
> [`query_manager/server_queries.rs:474`](../../crates/jazz-tools/src/query_manager/server_queries.rs#L474)

Example policies used in tests:

```rust
// INSERT only if session user exists in admins relation
with_insert(PolicyExpr::ExistsRel { rel: RelExpr::Filter { ... } })

// UPDATE with inherited parent check
with_update(
  Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
  PolicyExpr::Inherits { operation: Operation::Update, via_column: "parent_id".into(), max_depth: Some(10) }
)
```

> [`query_manager/rebac_tests.rs:1574`](../../crates/jazz-tools/src/query_manager/rebac_tests.rs#L1574)
> [`query_manager/rebac_tests.rs:1633`](../../crates/jazz-tools/src/query_manager/rebac_tests.rs#L1633)

## 13. Array Subqueries and Recursive Nodes as Meta Nodes

These are "meta nodes": they sit in the parent graph pipeline, but compute their output by compiling/settling child graphs.

Connection flow:

1. compile phase converts query specs into `SubgraphTemplate`s and attaches an `ArraySubqueryNode` / `RecursiveRelationNode` into the parent graph
2. settle phase reaches that node with parent input tuples
3. node instantiates child graph(s) with bound correlation values and settles them
4. child results are mapped back into parent tuples (array column values or recursive expansion rows)
5. parent graph continues downstream (filter/sort/limit/output) with those enriched tuples

> [`query_manager/graph.rs:896`](../../crates/jazz-tools/src/query_manager/graph.rs#L896)
> [`query_manager/graph.rs:988`](../../crates/jazz-tools/src/query_manager/graph.rs#L988)
> [`query_manager/graph.rs:1052`](../../crates/jazz-tools/src/query_manager/graph.rs#L1052)
> [`query_manager/graph.rs:1151`](../../crates/jazz-tools/src/query_manager/graph.rs#L1151)
> [`query_manager/graph.rs:2002`](../../crates/jazz-tools/src/query_manager/graph.rs#L2002)
> [`query_manager/graph.rs:1865`](../../crates/jazz-tools/src/query_manager/graph.rs#L1865)

Child-graph delegation points:

```rust
// ArraySubqueryNode
let instance = self.subgraph_template.instantiate(correlation_value.clone(), &self.schema)?;
let row_delta = instance.graph.settle(io, row_loader);

// RecursiveRelationNode (per frontier step)
let mut instance = self.step_template.instantiate(correlation_value.clone(), &self.schema)?;
instance.graph.settle(io, row_loader).added
```

> [`graph_nodes/array_subquery.rs:247`](../../crates/jazz-tools/src/query_manager/graph_nodes/array_subquery.rs#L247)
> [`graph_nodes/array_subquery.rs:258`](../../crates/jazz-tools/src/query_manager/graph_nodes/array_subquery.rs#L258)
> [`graph_nodes/recursive_relation.rs:406`](../../crates/jazz-tools/src/query_manager/graph_nodes/recursive_relation.rs#L406)
> [`graph_nodes/recursive_relation.rs:413`](../../crates/jazz-tools/src/query_manager/graph_nodes/recursive_relation.rs#L413)
> [`graph_nodes/subgraph.rs:55`](../../crates/jazz-tools/src/query_manager/graph_nodes/subgraph.rs#L55)
> [`graph_nodes/subgraph.rs:152`](../../crates/jazz-tools/src/query_manager/graph_nodes/subgraph.rs#L152)

Dependency connection: table changes in inner/step tables set `inner_dirty`, which causes full child-graph re-evaluation on the next settle.

> [`query_manager/graph.rs:1516`](../../crates/jazz-tools/src/query_manager/graph.rs#L1516)
> [`query_manager/graph.rs:1563`](../../crates/jazz-tools/src/query_manager/graph.rs#L1563)
> [`query_manager/graph.rs:2013`](../../crates/jazz-tools/src/query_manager/graph.rs#L2013)
> [`graph_nodes/array_subquery.rs:301`](../../crates/jazz-tools/src/query_manager/graph_nodes/array_subquery.rs#L301)
> [`graph_nodes/recursive_relation.rs:93`](../../crates/jazz-tools/src/query_manager/graph_nodes/recursive_relation.rs#L93)

Example queries from tests:

```rust
let q1 = qm.query("users")
  .with_array("posts", |sub| sub.from("posts").correlate("author_id", "users.id"))
  .build();

let q2 = qm.query("teams")
  .filter_eq("name", Value::Text("team-1".into()))
  .with_recursive(|r| {
    r.from("team_edges")
      .correlate("child_team", "_id")
      .select(&["parent_team"])
      .hop("teams", "parent_team")
      .max_depth(10)
  })
  .build();
```

> [`query_manager/manager_tests.rs:3476`](../../crates/jazz-tools/src/query_manager/manager_tests.rs#L3476)
> [`query_manager/manager_tests.rs:241`](../../crates/jazz-tools/src/query_manager/manager_tests.rs#L241)

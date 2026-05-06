# Life of a Query Subscription (Browser Main Thread -> Worker) — Status Quo

This walkthrough follows the most common browser setup:

- main thread runtime: in-memory and UI-facing
- worker runtime: persistent, OPFS-backed, and connected upstream

We will use two familiar calls:

```ts
const rows = await db.all(app.todos.where({ done: false }));

const unsubscribe = db.subscribeAll(app.todos.where({ done: false }), ({ all }) => {
  console.log(all.length);
});
```

The nice part is that both calls go through almost the same machinery.

## 1. The Query Starts as a Typed Table Expression

Application code builds queries from the generated `app` surface:

```ts
app.todos.where({ done: false }).orderBy("title");
```

That typed builder is turned into runtime query JSON before it ever reaches Rust/WASM.

## 2. `createDb(...)` Builds Two Browser Runtimes

In persistent browser mode, Jazz creates:

- a main-thread `JazzClient` backed by an in-memory runtime
- a worker-hosted `JazzClient` backed by `WasmRuntime.openPersistent(...)`

The worker is treated as the main runtime's upstream peer. That gives the main thread a simple API surface while leaving durable storage and network ownership in the worker.

## 3. One-Shot Query Path

`db.all(...)` feels like a read, but internally it still uses the subscription engine:

1. create a temporary subscription
2. settle it
3. wait for the first full snapshot at the requested durability tier
4. resolve the promise
5. tear the temporary subscription down

This shared path is why one-shot reads and live subscriptions agree on what the "current answer" means.

## 4. Live Subscription Path

`db.subscribeAll(...)` does the same initial setup, but it keeps the subscription registered.

After the first settle:

- local writes can re-settle it immediately
- worker replay can add remote rows
- policy/schema changes can recompile or re-filter it
- the callback receives only the changed rows plus the new `all` set

## 5. What the Main Thread Actually Does

The main runtime is still a full Jazz runtime. It can:

- compile local query graphs
- apply local row changes immediately
- deliver subscription callbacks synchronously

But in persistent browser mode it does not own durable storage. When it needs remote-tier or persistent data, it forwards sync/query state to the worker runtime.

## 6. What the Worker Actually Does

The worker runtime owns:

- OPFS-backed storage
- durable row histories and visible entries
- upstream app-scoped `/apps/<appId>/ws`
- query replay beyond the main-thread cache

So if the main thread subscribes to a query whose answer depends on worker or server state, the worker is the place that settles and relays that answer back.

## 7. First Delivery Is Tier-Aware

The first callback is not just "whenever we saw some rows".

If the subscription requests a durability tier, the runtime waits for `QuerySettled` at that tier before publishing the first result. That keeps the app-level story crisp:

- current data is local-first
- first delivery can still require worker/edge/global settlement when asked

## 8. Later Updates

Once the subscription is alive, updates can come from:

- local mutations
- worker replay
- upstream server changes
- schema activation
- permission changes

All of those eventually look the same to the app: the graph settles again, the `SubscriptionManager` computes `{ all, added, updated, removed }`, and your callback fires.

## One Short Flow Sketch

```text
db.subscribeAll(...)
  -> main runtime registers subscription
  -> worker/runtime sync path receives forwarded query state
  -> worker settles current answer from OPFS + upstream state
  -> worker sends row batch entries + QuerySettled
  -> main runtime updates its local graph
  -> callback receives first snapshot
```

`QuerySettled.scope` is the query-completeness signal. Batch durability travels separately as
`BatchFate`, and that fate applies to the whole batch.
The subscription path should not need a per-row decode of batch-fate member lists to decide whether
a known row has reached the requested tier.

## Key Files

| File                                               | Purpose                                       |
| -------------------------------------------------- | --------------------------------------------- |
| `packages/jazz-tools/src/runtime/db.ts`            | `Db` surface used by apps                     |
| `packages/jazz-tools/src/runtime/client.ts`        | `JazzClient` query/subscription orchestration |
| `packages/jazz-tools/src/runtime/worker-bridge.ts` | Main thread <-> worker bridge                 |
| `packages/jazz-tools/src/worker/jazz-worker.ts`    | Persistent worker host                        |
| `crates/jazz-tools/src/runtime_core.rs`            | Rust runtime entry points                     |
| `crates/jazz-tools/src/query_manager/manager.rs`   | Query settling and subscription updates       |

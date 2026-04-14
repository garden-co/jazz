# Browser Adapters — Status Quo

This doc describes how browser apps are wired to the Jazz runtime today.

The browser setup is intentionally split:

- the main thread gets a small, immediate, UI-friendly runtime
- a dedicated worker gets the durable runtime, OPFS access, and upstream sync connection

That arrangement is what lets Jazz keep its synchronous storage/query model in the browser without blocking rendering.

## High-Level Topology

```text
React/Vue/Svelte app
  -> createDb(config)
  -> Db
     -> main-thread JazzClient
        -> in-memory WasmRuntime
        -> WorkerBridge
           -> dedicated worker
              -> persistent WasmRuntime.openPersistent(...)
              -> OPFS-backed storage
              -> upstream /sync + /events
```

## Two Browser Modes

### Persistent mode

This is the default browser setup.

- main thread uses an in-memory runtime
- worker owns durable OPFS storage
- worker also owns upstream server connectivity

### Memory mode

This skips the worker entirely.

- one in-memory runtime
- no OPFS persistence
- direct remote sync when configured

It is useful for tests, demos, and environments that do not want a dedicated worker.

## What `Db` Owns

`Db` is the app-facing façade. It is responsible for:

- translating typed query builders into runtime queries
- creating or reusing `JazzClient` instances
- exposing `all`, `one`, `insert`, `update`, `delete`, and subscription APIs
- waiting for the worker bridge when a call needs worker-backed durability

From the application's point of view, it is just "the database object". Internally, it is the coordinator for the main-thread runtime plus any worker bridge.

## What `JazzClient` Owns

`JazzClient` is the runtime-facing client layer. It:

- wraps CRUD and query calls
- manages subscription lifecycles
- handles stream attachment and reconnect behavior
- decides when a one-shot remote-tier query must wait for sync attachment before it can return

This is the piece that makes browser, worker, and native runtimes look uniform from the TypeScript side.

## What the WorkerBridge Owns

`WorkerBridge` turns the dedicated worker into the main runtime's upstream peer.

It is responsible for:

- worker boot/init
- forwarding sync payloads over `postMessage`
- updating auth/session state in the worker
- shutdown and crash-simulation flows

The important architectural point is that the main runtime does not special-case OPFS. It talks to the worker through the same sync-shaped concepts the rest of the stack already uses.

## What the Worker Owns

The worker is the durable browser runtime host. It owns:

- `WasmRuntime.openPersistent(...)`
- OPFS-backed storage
- upstream `/events` connection
- upstream `/sync` POSTs
- replay of sync messages to the main thread runtime

That is why the browser architecture can stay faithful to the rest of Jazz. The worker is not just a storage helper; it is a real runtime tier.

## Common Flows

### Startup

1. `createDb(...)` decides whether to use worker-backed or memory mode.
2. Browser persistent mode spins up the worker and waits for it to report readiness.
3. The worker opens its persistent runtime and registers the main thread as a peer.
4. Normal query/mutation APIs can now use the same `Db` surface.

### Mutation

1. App calls `db.insert(...)` / `db.update(...)` / `db.delete(...)`.
2. Main-thread runtime applies the local write immediately.
3. Outbound sync is forwarded to the worker.
4. Worker persists and, when configured, forwards upstream.
5. Durable APIs resolve when the requested tier is confirmed.

### Query

1. App builds a typed query from `app.todos...`.
2. `Db` translates it into runtime query JSON.
3. Main runtime executes the query or subscription.
4. If the answer depends on worker or remote state, the worker path fills it in.

## Framework Bindings

The React/Vue/Svelte wrappers sit on top of the same runtime surface.

They mainly add:

- lifecycle management
- context/provider wiring
- ergonomic reactive hooks or stores

The browser architecture itself stays the same underneath.

## Key Files

| File                                                | Purpose                            |
| --------------------------------------------------- | ---------------------------------- |
| `packages/jazz-tools/src/runtime/db.ts`             | App-facing `Db` entry point        |
| `packages/jazz-tools/src/runtime/client.ts`         | `JazzClient` implementation        |
| `packages/jazz-tools/src/runtime/worker-bridge.ts`  | Main-thread to worker coordination |
| `packages/jazz-tools/src/worker/jazz-worker.ts`     | Dedicated worker runtime host      |
| `packages/jazz-tools/src/runtime/sync-transport.ts` | Shared transport utilities         |
| `crates/jazz-wasm/src/runtime.rs`                   | WASM runtime bindings              |

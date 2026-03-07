# Browser Adapters — Status Quo

This document describes the current TypeScript-side browser adapter architecture in `jazz-tools`, focused on how browser apps are wired to the WASM runtime, worker persistence, and sync transport.

## Scope

Primary code paths covered:

- `packages/jazz-tools/src/runtime/db.ts`
- `packages/jazz-tools/src/runtime/client.ts`
- `packages/jazz-tools/src/runtime/worker-bridge.ts`
- `packages/jazz-tools/src/worker/jazz-worker.ts`
- `packages/jazz-tools/src/worker/worker-protocol.ts`
- `packages/jazz-tools/src/runtime/sync-transport.ts`
- `packages/jazz-tools/src/runtime/local-auth.ts`
- `packages/jazz-tools/src/runtime/client-session.ts`
- `packages/jazz-tools/src/react/provider.tsx`
- `packages/jazz-tools/src/react/use-all.ts`
- `packages/jazz-tools/tests/browser/worker-bridge.test.ts`

Related status-quo docs:

- `specs/status-quo/storage.md`
- `specs/status-quo/http_transport.md`
- `specs/status-quo/ts_client_codegen.md`

## High-Level Topology

```text
React App
  -> JazzProvider
  -> createDb(config)
  -> Db (main thread facade)
     -> JazzClient (main thread in-memory WasmRuntime)
     -> WorkerBridge (postMessage)
        -> jazz-worker.ts (dedicated worker)
           -> persistent WasmRuntime.openPersistent(..., "worker")
           -> OPFS durability
           -> upstream /sync POST + /events binary stream
```

In browser mode, there are two runtime instances:

- Main thread runtime: in-memory, immediate local API surface.
- Worker runtime: persistent OPFS runtime, also owns upstream server connectivity.

With `driver: { type: "memory" }`, browser apps skip worker/OPFS entirely and run only the
main-thread in-memory runtime with direct remote sync.

## Public Entry Points

`createDb(config)` in `packages/jazz-tools/src/runtime/db.ts` is the primary app entry point. It chooses mode with:

- `driver: { type: "persistent" }` (default when omitted):
  - Browser path: `Db.createWithWorker(...)` when both `window` and `Worker` exist.
  - Non-browser path: `Db.create(...)`.
- `driver: { type: "memory" }`:
  - Always `Db.create(...)` (no worker, no OPFS, no tab leader election).
  - Requires `serverUrl` (validated at `createDb`).

In memory mode, default durability tier is `edge` when `serverUrl` is configured.

`JazzProvider` in `packages/jazz-tools/src/react/provider.tsx` wraps this and exposes:

- `useDb()` for imperative mutations/queries.
- `useAll()` in `packages/jazz-tools/src/react/use-all.ts` for reactive subscriptions via `useSyncExternalStore`.

## Current Adapter Surfaces (React + TypeScript)

### React Adapter (`jazz-tools/react`)

Export surface is defined in `packages/jazz-tools/src/react/index.ts`:

- `JazzProvider`, `useDb`, `useSession`
- `useAll`
- `useLinkExternalIdentity`
- Synthetic user UI/helpers (`SyntheticUserSwitcher`, storage helpers)

Current behavior:

1. `JazzProvider` (`react/provider.tsx`) resolves local-auth defaults, then runs `Promise.all([createDb(config), resolveClientSession(config)])` on mount.
2. Provider context stores `{ db, session }`; `useDb()` and `useSession()` read from this context and throw outside provider boundaries.
3. On unmount, provider calls `db.shutdown()` for clean worker/runtime teardown.
4. `useAll(query)` (`react/use-all.ts`) wraps `db.subscribeAll(...)` and streams reactive updates.
5. `useLinkExternalIdentity` (`react/use-link-external-identity.ts`) bridges local synthetic identity to external JWT identity and can fall back to active synthetic profile state.

### TypeScript Adapter (`jazz-tools` / `jazz-tools/backend`)

`packages/jazz-tools/src/index.ts` re-exports runtime APIs from `runtime/index.ts`, and `packages/jazz-tools/src/backend/index.ts` currently re-exports the same runtime surface.

Primary TypeScript-facing APIs:

- `createDb`, `Db`, `QueryBuilder`, `TableProxy`
- `JazzClient`, `SessionClient`, `resolveClientSession`
- Query/value adapters used internally by `Db` (`translateQuery`, `transformRows`, `toValueArray`, `toUpdateRecord`)

Current plain TypeScript usage pattern (see `examples/todo-client-localfirst-ts/src/main.ts`):

1. Build a `DbConfig` (app/env/branch/auth/tier/server options + optional `driver` mode).
2. Initialize with `Promise.all([createDb(config), resolveClientSession(config)])`.
3. Read via `db.all(...)`/`db.one(...)` or `db.subscribeAll(...)`.
4. Mutate via local-first APIs (`insert`, `update`, `delete`) or durable variants (`insertDurable`, `updateDurable`, `deleteDurable`) when tiered acknowledgement matters.
5. Tear down with `db.shutdown()`.

## Runtime Layers and Responsibilities

1. `Db` (`runtime/db.ts`)

- High-level typed API (`insert`, `update`, `delete`, `insertDurable`, `updateDurable`, `deleteDurable`, `all`, `one`, `subscribeAll`).
- Creates and memoizes `JazzClient` per schema key.
- Creates worker + bridge in browser mode.
- Waits for bridge init before durability-tiered mutations.

2. `JazzClient` (`runtime/client.ts`)

- Runtime-agnostic client over a `Runtime` interface.
- Wraps CRUD/query/subscription APIs.
- Handles upstream sync transport wiring (`onSyncMessageToSend`, binary stream reader, reconnect/backoff).

3. `WorkerBridge` (`runtime/worker-bridge.ts`)

- Main-thread bridge adapter between runtime outbox/inbox and worker `postMessage`.
- Treats worker as the main runtime's "server" by calling `runtime.addServer()`.

4. Worker runtime host (`worker/jazz-worker.ts`)

- Bootstraps `jazz-wasm`.
- Opens persistent runtime with `WasmRuntime.openPersistent(...)` and tier `"worker"`.
- Registers main thread as peer client (`addClient`, `setClientRole("peer")`).
- Routes sync envelopes to either main thread or upstream server.
- Manages stream reconnect/backoff, shutdown, and crash simulation behavior.

5. Shared transport (`runtime/sync-transport.ts`)

- URL/path-prefix building.
- Auth header policy.
- `/sync` POST helper.
- Binary frame parser for `/events`.

## Message Protocol Between Main Thread and Worker

Defined in `packages/jazz-tools/src/worker/worker-protocol.ts`.

Main -> Worker messages:

- `init`
- `sync`
- `update-auth`
- `shutdown`
- `simulate-crash`

Worker -> Main messages:

- `ready`
- `init-ok`
- `sync`
- `error`
- `shutdown-ok`

Payloads are JSON strings for sync messages (`payload: string`).

## Lifecycle Flows

### 1. Browser Startup

1. `createDb()` detects browser and calls `Db.createWithWorker()`.
2. Main thread loads WASM module and spawns `jazz-worker`.
3. Worker sends `ready` after loading WASM.
4. On first schema use, `Db.getClient(schema)` creates main-thread `JazzClient`.
5. `WorkerBridge.init(...)` sends `init` with schema/config/auth.
6. Worker opens persistent runtime (`openPersistent(..., "worker")`), registers main client, sets outbox handler, drains buffered sync messages, then sends `init-ok`.

### 2. Mutation Path

Durability mutation (`insertDurable`, `updateDurable`, `deleteDurable`):

1. `Db` waits for bridge init (`ensureBridgeReady()`).
2. Call uses the main-thread `JazzClient` durable mutation API.
3. Local runtime applies the write immediately and emits server-destination sync envelope.
4. `WorkerBridge` forwards envelope payload as `sync` message to worker.
5. Worker ingests message as peer-client sync input and may emit upstream server sync and/or client sync updates.
6. Promise resolves when the requested durability tier (`worker`, `edge`, `global`) is acknowledged.

### 3. Query Path

1. Query builder JSON is produced by generated code.
2. `translateQuery()` converts builder JSON to runtime query JSON.
3. `JazzClient.query()` executes query against main runtime.
4. Rows are converted to typed JS objects with `transformRows()`.

### 4. Subscription Path

1. `Db.subscribeAll()` subscribes on main runtime.
2. Runtime callback emits row deltas.
3. `SubscriptionManager` computes `{ all, added, updated, removed }`.
4. React `useAll()` pushes updates via `useSyncExternalStore`.

### 5. Upstream Sync Path (Worker-Owned in Browser)

Outgoing:

1. Worker runtime emits server-destination sync envelope.
2. Worker sends `/sync` POST via `sendSyncPayload()` for standard payloads; catalogue payloads are only POSTed when `adminSecret` is configured, otherwise they are dropped client-side.
3. Auth headers are selected by payload type and auth context.

Incoming:

1. Worker opens `/events?client_id=...` with `fetch`.
2. `readBinaryFrames()` parses length-prefixed JSON frames.
3. `Connected` event updates client id and re-attaches server in runtime.
4. `SyncUpdate` events are applied to worker runtime.
5. Worker runtime emits client-destination envelopes, forwarded to main thread as `sync`.

Reconnect:

- Exponential backoff with jitter in both main client and worker code paths.
- Re-attach (`addServer`) is used as subscription replay boundary.

### 6. Shutdown and Crash Simulation

`Db.shutdown()`:

1. Waits for bridge init completion.
2. Sends worker `shutdown` and awaits `shutdown-ok` (best effort timeout).
3. Shuts down all memoized clients.
4. Terminates worker.

Worker `shutdown`:

- Aborts stream/reconnect timers.
- Detaches server.
- Flushes runtime and frees runtime handles.
- Posts `shutdown-ok` and closes worker global scope.

Worker `simulate-crash` (tests):

- Flushes WAL only (`flushWal`), skips clean snapshot flush, frees runtime, emits `shutdown-ok`.
- Used by browser tests to verify WAL-based recovery after reopen.

## Auth and Session Resolution

### Runtime Transport Auth

From `sync-transport.ts`:

- User auth precedence: JWT bearer first, then local auth headers.
- Catalogue payloads use `X-Jazz-Admin-Secret` and are skipped entirely when no admin secret is configured.
- Optional path prefix support for multi-tenant routing.

### Local Auth Defaults

From `local-auth.ts`:

- If no auth is configured and browser storage exists, defaults to local anonymous mode.
- Per-app token is generated/persisted in `localStorage`.

### Session for Permission Checks

From `client-session.ts`:

- JWT payload is decoded to derive session principal.
- Otherwise local mode/token derives a deterministic local principal id (`local:<hash>`).

## Platform Split Today

Browser (`runtime/db.ts`):

- Main-thread in-memory runtime + dedicated worker persistent runtime.
- Worker owns OPFS and upstream sync.

React Native (`react-native/db.ts`, `react-native/jazz-rn-runtime-adapter.ts`):

- Separate runtime adapter (`JazzRnRuntimeAdapter`) over `jazz-rn` binding.
- No web worker/OPFS path.

Node/non-browser:

- Single in-memory runtime path from `createDb()` fallback.

## Current Architectural Constraints

These are current design facts that matter for a redesign:

1. Browser mode depends on a dual-runtime bridge model.
2. Worker bridge is initialized once and bound to first client/runtime instance; `Db` memoizes clients per schema.
3. Worker protocol is JSON-string payload based with `any` runtime types in worker host.
4. Auth refresh plumbing exists (`update-auth`) but is not currently invoked by `Db`/React surfaces.
5. `createDb()` browser detection is environment-heuristic (`window` + `Worker`).
6. Transport logic is duplicated conceptually across main-thread client and worker runtime host (both own reconnect/stream attach semantics).
7. The `StorageDriver` hook is effectively minimal in this stack (mostly lifecycle close path).

## Test-Validated Behavior

Browser integration tests (`packages/jazz-tools/tests/browser/worker-bridge.test.ts`) currently validate:

- Browser worker initialization.
- Sync local CRUD and query semantics.
- OPFS persistence across shutdown/reopen.
- WAL recovery after simulated crash.
- Worker-tier ack behavior.
- Subscription updates through bridge.
- End-to-end server sync via worker path.

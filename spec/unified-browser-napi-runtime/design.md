# Unified NAPI Runtime for Browser via `wasm32-wasip1-threads`

## Overview

This document is a corrected design for replacing the browser-specific `jazz-wasm` + dedicated-worker bridge stack with a browser build of `jazz-napi`.

The original proposal had the right end goal: one Rust runtime surface across Node and browser, no app-owned `WorkerBridge`, and event-loop-safe subscription delivery. The main corrections are about what must change first to make that architecture real in this codebase.

### What stays from the original proposal

- One public Rust runtime package for native and browser targets.
- `NapiScheduler` / `ThreadsafeFunction` remains the mechanism that schedules `batched_tick()` onto the JS event loop.
- Multi-tab leader election remains in TypeScript.
- Cross-origin isolation is a hard requirement for the threaded browser build.
- Inbound sync messages must reach `RuntimeCore` on the JS event loop.
- Browser network transport can remain in JS and still meet the worker-bridge-removal goal.

### What changes in this revised design

1. The browser migration does **not** eliminate workers entirely.
   It eliminates the app-owned dedicated worker, `worker-bridge.ts`, and `worker-protocol.ts`.
   It does **not** eliminate workers under the hood, because:
   - `wasm32-wasip1-threads` depends on `SharedArrayBuffer` + worker-backed threading.
   - OPFS `FileSystemSyncAccessHandle` is only available in dedicated workers.

2. Tokio multi-thread on browser WASM is **not** assumed as a foundation.
   The current Tokio docs still mark WASM support as limited and reject `rt-multi-thread` without `tokio_unstable`.
   This design therefore treats Tokio-on-browser as optional follow-up work, not as the first architectural dependency.

3. The current `RuntimeCore` and `Storage` model cannot directly support the proposed persistence actor.
   Today, writes call into synchronous `Storage` immediately during `insert`, `update`, `delete`, and `persist_schema`.
   That means the browser runtime cannot simply "move OPFS into a background Rust thread" without first introducing a persistence command layer or another browser-specific storage split.

4. The browser runtime should keep an async bootstrap path.
   The current `jazz-wasm` flow already requires async module loading and persistent runtime open.
   The browser `jazz-napi` path should keep `Db.create()` async instead of promising a fully synchronous `new NapiRuntime(...)` path.

5. Multi-tab message passing remains.
   The current leader/follower `BroadcastChannel` protocol still exists after this migration.
   Only the main-thread <-> worker protocol disappears.

6. Browser network transport should stay in JavaScript.
   The repo already has a shared JS sync transport (`sync-transport.ts`) that works against the abstract runtime interface.
   Keeping fetch/stream handling in JS removes duplicated worker transport code without introducing new browser-WASM HTTP risk.

## Problem Statement

The current browser architecture has two public runtime surfaces:

- `jazz-wasm` for browser/worker execution
- `jazz-napi` for native Node execution

On the browser side, `packages/jazz-tools/src/runtime/db.ts` creates:

- a main-thread in-memory `WasmRuntime`
- a dedicated worker running a second persistent `WasmRuntime`
- a `WorkerBridge` plus `worker-protocol.ts` for sync message exchange

This split exists because:

- OPFS sync access is worker-only
- `jazz-wasm` is single-threaded
- the current `Storage` trait is synchronous and thread-local

The desired end state is still valid:

- one JS-facing runtime package
- one Rust runtime codebase
- no app-managed `WorkerBridge`
- background browser persistence implemented inside Rust-owned threads/channels instead of TS `postMessage`
- browser network sync retained in the existing JS transport layer

But reaching that end state requires an intermediate refactor of the browser persistence boundary.

## Goals

- Replace `jazz-wasm` as the browser runtime package with a browser build of `jazz-napi`.
- Remove `packages/jazz-tools/src/runtime/worker-bridge.ts`.
- Remove `packages/jazz-tools/src/worker/worker-protocol.ts`.
- Remove `packages/jazz-tools/src/worker/jazz-worker.ts`.
- Keep the `Db` and `JazzClient` public TypeScript API stable.
- Keep subscription callbacks and `batched_tick()` on the JS event loop.
- Preserve browser persistence and sync feature parity before deleting the old stack.

## Non-Goals

- Removing worker usage from the browser platform entirely.
- Making browser runtime construction fully synchronous.
- Moving multi-tab coordination out of TypeScript.
- Replacing native `jazz-napi` targets or server-side usage.
- Assuming Tokio multi-thread is production-ready on browser WASM before a proof-of-concept.

## Architecture / Components

### 1. Packaging and Loader

#### Current constraint

`crates/jazz-napi/package.json` currently publishes native targets only, and `crates/jazz-napi/index.js` is a Node/CommonJS loader that starts with `require('node:fs')` and branches on `process.platform`.

That is not a browser-ready package shape.

#### Design

Extend `jazz-napi` into a dual-environment package:

- native targets stay unchanged
- add a `wasm32-wasip1-threads` browser build
- publish the generated WASI/wasm artifacts
- expose a browser-safe entry path

Example target config:

```json
{
  "napi": {
    "binaryName": "jazz-napi",
    "targets": [
      "x86_64-unknown-linux-gnu",
      "x86_64-pc-windows-msvc",
      "x86_64-apple-darwin",
      "aarch64-apple-darwin",
      "wasm32-wasip1-threads"
    ]
  }
}
```

Additional package work required:

- include generated WASM/WASI loader artifacts in `files`
- add browser-aware exports instead of relying only on the current CommonJS loader
- update `pnpm-workspace.yaml` to install `wasm32` architecture packages

Required workspace config:

```yaml
supportedArchitectures:
  cpu:
    - current
    - wasm32
```

#### Why this choice

The original spec treated browser import as if the current `jazz-napi` package already supported it. It does not. Loader and package-manager changes are mandatory before any runtime migration can work in the browser.

### 2. JS-Facing Runtime Contract

#### Current constraint

`packages/jazz-tools/src/runtime/client.ts` has a shared `Runtime` interface, but the browser path still explicitly loads `jazz-wasm` via `loadWasmModule()`.
`crates/jazz-napi/index.d.ts` also exposes a Node-shaped constructor based on filesystem persistence, not a browser-persistent open path.

#### Design

Keep the TypeScript `Runtime` interface stable, but introduce an async browser runtime factory in `jazz-napi`:

```ts
export async function openBrowserRuntime(args: {
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  persistence: "memory" | "leader-opfs";
  tier?: "worker" | "edge" | "global";
}): Promise<Runtime>;
```

The browser path in `Db.create()` becomes:

```ts
const { openBrowserRuntime } = await import("jazz-napi");
const runtime = await openBrowserRuntime({
  schemaJson,
  appId,
  env,
  userBranch,
  dbName,
  persistence,
  tier,
});
```

#### Why this choice

Keeping async bootstrap is the least risky choice because:

- WASM module setup is async
- persistent browser bootstrap is async
- the current `Db.create()` API is already async

This preserves the public SDK shape while avoiding a fake "sync constructor" promise that the browser cannot honor.

### 3. Event-Loop-Owned Core and `NapiScheduler`

#### Design

The authoritative rule remains:

- all `RuntimeCore` mutations happen on the JS event loop
- `batched_tick()` runs on the JS event loop
- subscription callbacks run on the JS event loop

The existing `NapiScheduler` + `ThreadsafeFunction` design is retained for browser and native builds.

If background work receives inbound events, it must re-enter through a `ThreadsafeFunction`:

```rust
enum RuntimeInboundEvent {
    PersistenceAck { sequence: u64 },
    LifecycleHint(LifecycleHint),
}
```

The event-loop callback applies the event and then schedules `batched_tick()`.

#### Why this choice

This preserves the same safety property the current design relies on:

- JS-initiated CRUD never contends with background threads for the core lock
- React subscriptions continue to resolve from event-loop-owned state

### 4. Persistence Architecture

#### Current constraint

The current `Storage` trait is synchronous and explicitly single-threaded.
`RuntimeCore` calls storage directly during writes:

- `insert`
- `update`
- `delete`
- `persist_schema`
- `batched_tick()` durability flush

The current browser OPFS implementation also hard-requires a worker context.

#### Design

Introduce a browser-specific split between:

- event-loop-owned materialized runtime state
- worker-owned durable persistence actor

The persistence actor does **not** expose `Storage` directly to the JS event loop.
Instead, the runtime emits immutable persistence commands:

```rust
enum PersistenceCommand {
    ApplyBatch {
        sequence: u64,
        ops: Vec<PersistOp>,
    },
    Flush {
        sequence: u64,
    },
    Shutdown,
}
```

`PersistOp` represents durable writes that can be replayed in OPFS without borrowing the core:

```rust
enum PersistOp {
    CreateObject { id: ObjectId, metadata: HashMap<String, String> },
    AppendCommit { object_id: ObjectId, branch: String, commit: Commit },
    DeleteCommit { object_id: ObjectId, branch: String, commit_id: CommitId },
    SetBranchTails { object_id: ObjectId, branch: String, tails: Option<Vec<CommitId>> },
    StoreAckTier { commit_id: CommitId, tier: DurabilityTier },
    AppendCatalogueManifestOp { app_id: ObjectId, op: CatalogueManifestOp },
    IndexInsert { table: String, column: String, branch: String, value: Value, row_id: ObjectId },
    IndexRemove { table: String, column: String, branch: String, value: Value, row_id: ObjectId }
}
```

The browser-persistent runtime therefore needs a refactor in one of these shapes:

- preferred: `RuntimeCore` writes into an in-memory materialized store and emits `PersistenceCommand` batches for background durability
- not preferred: actor-proxy `Storage` backed by synchronous cross-thread RPC

The actor-proxy option is rejected because it would still force the main thread to wait on worker-owned storage for every write and would recreate the current latency/lock boundary under a different name.

#### Performance Constraint

The worker boundary is **not** allowed to sit on the steady-state read path.

That means:

- queries do not call into the persistence actor
- index lookups do not call into the persistence actor
- full index scans do not stream back from the persistence actor
- React subscriptions do not depend on worker round-trips

The persistence actor is write-behind and recovery-oriented only.

Steady-state reads must come from the event-loop-owned in-memory runtime state and indexes.
If a full scan is needed for a user query, it happens against the in-memory runtime, not OPFS.

The only times large data movement from the persistence actor is acceptable are:

- cold start hydration
- crash recovery / WAL replay
- explicit maintenance or debug operations

Even in those cases, communication must be bulk-oriented:

- send coarse batches, not row-by-row callbacks
- avoid per-record TSFN crossings
- yield between batches so startup work does not monopolize the event loop

#### Why this choice

This is the central correction in the design.

Without this split, the browser runtime would still need direct synchronous storage access on the JS thread, which is incompatible with worker-only OPFS.
It also prevents us from accidentally turning the persistence worker into a remote query engine, which would make full scans and index-heavy reads much slower.

### 5. Keep Browser Sync Transport in JavaScript

#### Design

Reuse the existing JavaScript sync transport path in:

- `packages/jazz-tools/src/runtime/client.ts`
- `packages/jazz-tools/src/runtime/sync-transport.ts`

The browser runtime continues to expose:

```ts
runtime.onSyncMessageToSend(callback);
runtime.onSyncMessageReceived(payload);
```

Outgoing flow:

```text
RuntimeCore outbox
  -> runtime.onSyncMessageToSend(...)
  -> sendSyncPayload(...)
  -> fetch POST /sync
```

Incoming flow:

```text
fetch /events
  -> readBinaryFrames(...)
  -> runtime.onSyncMessageReceived(payload)
  -> batched_tick()
  -> subscriptions / React updates
```

The worker-specific transport in `packages/jazz-tools/src/worker/jazz-worker.ts` is deleted once the browser path uses `jazz-napi` directly.

#### Why this choice

This is the lower-risk and more coherent design for this repo:

- `sync-transport.ts` is already shared logic
- `JazzClient` already knows how to drive runtime sync over JS callbacks
- auth refresh, reconnect, path-prefix handling, and stream framing already exist in one place
- it removes worker-only transport duplication without forcing browser HTTP into the WASM runtime

The trade-off is intentional: browser transport is not fully unified with a hypothetical Rust-native transport, but the migration stays focused on the real architectural problem, which is persistence and worker ownership.

#### Future Improvement

After the browser `jazz-napi` runtime is stable, we can evaluate moving only the browser fetch/stream transport into Rust while keeping the same event-loop ownership rules for `RuntimeCore`.

That follow-up would be limited to:

- HTTP `POST /sync`
- `/events` stream connection and frame reading
- callback/TSFN handoff back onto the JS event loop for `onSyncMessageReceived`

It is intentionally deferred because it is not required to remove the worker bridge, and it would add browser-WASM transport complexity during the riskiest phase of the migration.

### 6. Multi-Tab Coordination

#### Design

Keep `TabLeaderElection` and the `BroadcastChannel` relay in TypeScript.

Roles:

- leader tab: owns persistent browser runtime and OPFS actor
- follower tabs: use memory-backed runtime and receive peer sync from leader over `BroadcastChannel`

This means the migration deletes:

- `worker-bridge.ts`
- `worker-protocol.ts`
- `jazz-worker.ts`

But it does **not** delete:

- leader election
- leader/follower relay messages
- follower catch-up logic

Representative relay model:

```ts
type TabRelayMessage =
  | { type: "follower-sync"; fromTabId: string; toLeaderTabId: string; payload: Uint8Array[] }
  | { type: "leader-sync"; fromLeaderTabId: string; toTabId: string; payload: Uint8Array[] }
  | { type: "follower-close"; fromTabId: string; toLeaderTabId: string };
```

#### Why this choice

The original spec said "no message protocol." That is only true for the main-thread <-> dedicated-worker bridge.
Cross-tab coordination is still a message protocol and should be described as such.

### 7. Lifecycle and Shutdown

#### Design

Lifecycle hints remain part of the runtime surface:

```ts
runtime.onLifecycleHint("visibility-hidden");
runtime.onLifecycleHint("pagehide");
runtime.onLifecycleHint("freeze");
runtime.onLifecycleHint("resume");
```

`Db` fan-outs those hints to:

- the Rust runtime, for persistence-related behavior
- the JS sync controller, for reconnect/stream lifecycle behavior

The Rust side forwards lifecycle hints to background persistence actors so they can:

- flush pending persistence commands
- perform fast shutdown when the leader tab exits

#### Why this choice

The old worker bridge used postMessage for lifecycle. The new design keeps the behavior but removes the app-managed bridge dependency.

### 8. Tooling and Headers

#### Design

Threaded browser builds require:

- HTTPS or another secure context
- cross-origin isolation

Provide repo-level dev middleware for:

- `Cross-Origin-Opener-Policy: same-origin`
- `Cross-Origin-Embedder-Policy: require-corp`

Do **not** delete `vite-plugin-wasm` or `vite-plugin-top-level-await` in the design phase.
Those removals happen only after the `jazz-napi` browser package is proven to bundle cleanly in this repo.

#### Why this choice

Bundle/tooling cleanup should follow a working browser package, not precede it.

## Data Models

### `BrowserRuntimeMode`

```rust
enum BrowserRuntimeMode {
    MemoryOnly,
    LeaderPersistent { db_name: String },
    FollowerMemory { leader_tab_id: String },
}
```

Purpose:

- captures whether the current tab owns OPFS
- avoids implicit branching through scattered `isLeader` checks

### `PersistenceCommand`

See the persistence section above.

Purpose:

- decouples event-loop mutation from worker-only durability
- gives the system a testable, replayable boundary

### `RuntimeInboundEvent`

See the scheduler section above.

Purpose:

- unifies events arriving from background persistence/lifecycle channels
- ensures non-JS background state changes re-enter through the event loop

### `TabRelayMessage`

See the multi-tab section above.

Purpose:

- explicitly documents the protocol that remains after deleting the worker bridge

## Migration Plan

### Phase 0: Browser Feasibility Spike

Validate the following before large refactors:

1. `jazz-napi` browser package loads in this repo via Vite.
2. `NapiScheduler` / `ThreadsafeFunction` can schedule a browser-side `batched_tick()`.
3. `std::thread::spawn` works for a minimal browser worker task under `wasm32-wasip1-threads`.
4. cross-origin isolation is correctly applied in local dev and browser tests.

Exit criteria:

- browser test can instantiate `jazz-napi` and run in-memory CRUD + subscription callback

### Phase 1: Browser In-Memory Runtime on `jazz-napi`

- add browser build target and loader/package changes
- route browser `Db.create()` through `jazz-napi`
- keep `client.ts` / `sync-transport.ts` as the browser network layer
- keep persistence disabled or memory-only in this phase
- keep `Db`/`JazzClient` API unchanged

Exit criteria:

- `jazz-tools` browser tests pass using `jazz-napi` memory mode
- existing JS sync transport works unchanged against the new runtime

### Phase 2: Persistence Command Refactor

- introduce `PersistenceCommand` and `PersistOp`
- make browser runtime emit durable command batches instead of directly owning OPFS storage on the JS thread
- add worker-owned OPFS actor for leader tabs only
- keep followers in memory with leader relay

Exit criteria:

- insert -> close -> reopen -> query works in leader tab
- follower tabs continue to receive local updates from leader relay

### Phase 3: Simplify to One JS Browser Sync Path

- keep `client.ts` + `sync-transport.ts` as the only browser HTTP transport path
- delete worker-specific sync code from `jazz-worker.ts`
- ensure leader/follower tab routing still works with direct runtime sync callbacks
- preserve JS event-loop ownership of `batched_tick()`

Exit criteria:

- two-browser-client sync test passes without the old dedicated worker stack
- no browser HTTP logic remains in the deleted worker implementation

### Phase 4: Delete Old Browser Worker Stack

- delete `crates/jazz-wasm`
- delete `packages/jazz-tools/src/worker/jazz-worker.ts`
- delete `packages/jazz-tools/src/worker/worker-protocol.ts`
- delete `packages/jazz-tools/src/runtime/worker-bridge.ts`
- delete `loadWasmModule()` browser path
- remove now-unused bundler dependencies after verification

Exit criteria:

- browser test suite passes without `jazz-wasm` or the worker bridge

## Testing Strategy

### Package and Loader Tests

- verify browser bundle resolves `jazz-napi` correctly
- verify `pnpm` workspace installs `wasm32` package artifacts
- verify failure mode without cross-origin isolation is explicit and actionable

### Runtime Invariant Tests

- CRUD from JS thread never blocks on a background-held core lock
- subscription callbacks run on the JS event loop
- `createSubscription()` remains sync and zero-work
- `executeSubscription()` remains deferred

Representative browser test:

```ts
it("delivers subscription callbacks after a scheduled tick", async () => {
  const db = await createDb(config);
  const events: unknown[] = [];

  db.from(todos).subscribeAll((rows) => {
    events.push(rows);
  });

  db.insertInto(todos).values({ title: "A" }).exec();

  await waitFor(() => expect(events.length).toBeGreaterThan(0));
});
```

### Persistence Tests

- leader tab opens persistent runtime
- writes emit persistence commands in order
- OPFS actor replays commands and flushes successfully
- reopen hydrates durable state into runtime
- follower tabs do not attempt OPFS open
- steady-state reads do not require worker round-trips

Representative integration test:

```ts
it("persists leader-tab writes across reopen", async () => {
  const db1 = await createDb({ ...config, dbName: "spec-persist" });
  db1.insertInto(todos).values({ title: "persisted" }).exec();
  await db1.close();

  const db2 = await createDb({ ...config, dbName: "spec-persist" });
  const rows = await db2.from(todos).all();
  expect(rows.some((row) => row.title === "persisted")).toBe(true);
});
```

### Multi-Tab Tests

- leader election still converges
- follower writes relay to leader
- leader relays sync payloads back to followers
- leader shutdown promotes a follower cleanly

### Network Sync Tests

- outgoing payloads leave the runtime through `onSyncMessageToSend`
- incoming stream frames call `runtime.onSyncMessageReceived(...)`
- reconnect/backoff remains owned by the JS sync controller
- two clients converge through server sync

### Performance Tests

- verify local reads and subscriptions complete without persistence-worker participation
- measure full in-memory scan performance separately from cold-start hydration performance
- verify hydration applies data in bounded batches rather than one callback per record

### Regression Tests

- backend `jazz-napi` native usage remains unchanged
- React Native path remains unchanged
- package publish output includes both native and browser artifacts

## Risks and Open Questions

1. Browser `jazz-napi` packaging may require export-map and publish-layout work beyond adding a target line.
2. `ThreadsafeFunction` behavior on browser WASM needs a repo-local proof before committing the migration.
3. The persistence-command refactor touches core write paths and is the highest-risk implementation area.
4. Leader/follower routing needs a clear ownership model once HTTP stays in JS but persistence moves behind a Rust actor.
5. Bundle size is likely to increase and must be measured before deleting the old path.

## References

- [NAPI-RS WebAssembly docs](https://napi.rs/docs/concepts/webassembly)
- [Tokio WASM support docs](https://docs.rs/tokio/latest/src/tokio/lib.rs.html)
- [MDN: FileSystemSyncAccessHandle](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle)
- [MDN: SharedArrayBuffer](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer)
- [MDN: Fetch API](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API)

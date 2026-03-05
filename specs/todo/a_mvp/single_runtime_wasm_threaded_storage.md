# Single Runtime with WASM-Threaded Storage

## Status

Proposed.

## Problem

The browser architecture runs **two separate RuntimeCore instances** connected by a JS bridge:

```
MAIN THREAD                           DEDICATED WORKER
┌─────────────────────────┐          ┌──────────────────────────┐
│ RuntimeCore             │          │ RuntimeCore              │
│  (MemoryStorage)        │  post    │  (OpfsBTreeStorage/OPFS) │
│  queries, subscriptions │◄─Message─►  upstream server conn    │
│  React/UI bindings      │          │  peer routing            │
└─────────────────────────┘          └──────────────────────────┘
         WorkerBridge                       jazz-worker.ts
```

This dual-runtime design exists for one reason: OPFS `FileSystemSyncAccessHandle` is only available in Dedicated Workers. But it brings significant costs:

1. **Duplicated state.** Every object lives in both MemoryStorage (main) and OpfsBTreeStorage (worker). Double the RAM.
2. **Sync protocol overhead.** Mutations round-trip through postMessage serialization. Every write goes main→worker→main before the subscription fires.
3. **Complexity.** `WorkerBridge` (400 LOC), `jazz-worker.ts` (650 LOC), `worker-protocol.ts` — all exist solely to glue two runtimes together. State machines, timeout handling, lifecycle forwarding, peer routing duplication.
4. **Latency.** postMessage is not free. Batch flushing via `queueMicrotask` adds a tick of delay in each direction.
5. **Two outbox routers.** The worker re-implements sync routing (server, client, peer destinations) that RuntimeCore already handles. The main thread's `createSyncOutboxRouter` is a third routing layer.

## Goal

**One RuntimeCore on the main thread.** Storage I/O happens on an internal worker thread spawned by Rust/WASM. The JS layer sees a single runtime — no bridge, no protocol, no second runtime.

```
MAIN THREAD
┌──────────────────────────────────────────┐
│ RuntimeCore                              │
│  (ThreadedOpfsStorage)                   │
│  queries, subscriptions, server sync     │
│  React/UI bindings                       │
│                                          │
│  Storage calls ──► command channel ──┐   │
│                                      │   │
│  ┌───────────────────────────────────┼── │ ──┐
│  │ INTERNAL WORKER THREAD            ▼      │
│  │  OpfsBTreeStorage                        │
│  │  FileSystemSyncAccessHandle              │
│  │  (runs SyncFile ops, returns results)    │
│  └──────────────────────────────────────────┘
└──────────────────────────────────────────┘
```

## Design

### Core idea: async Storage adapter over a sync worker

The `Storage` trait is synchronous today — `index_range()` returns rows immediately. This is the key design tension: the main thread can't call OPFS synchronously, but the query engine demands synchronous reads.

The solution has two parts:

1. **Cache in main-thread memory, persist on the worker thread.** The main-thread storage adapter holds a `BTreeMap<Vec<u8>, Vec<u8>>` that serves all reads synchronously. No page layout, no superblock, no file abstraction — just a plain sorted map. `BTreeMap` over `HashMap` because `index_range` needs prefix-based range scans. Writes go to both the in-memory map and (asynchronously) to the OPFS worker.

2. **On startup, rehydrate from OPFS.** The worker thread reads the full persistent state and sends it to the main thread before the runtime becomes ready. After that, the in-memory cache is the source of truth for reads; the OPFS worker is a durable write-behind replica.

This is **not** a general async-storage abstraction. It's a concrete "memory + write-behind OPFS" composite that satisfies the synchronous `Storage` trait.

### Threading mechanism: `wasm-bindgen` Worker spawning

WASM threads via `SharedArrayBuffer` + `Atomics` require COOP/COEP headers and have compatibility concerns. Instead, we use a simpler approach:

**Spawn a Dedicated Worker from Rust** using `web_sys::Worker`. Communication uses `postMessage` — but this is a **private internal channel** between Rust on the main thread and Rust on the worker thread, not a JS-level protocol.

The worker thread:

- Loads the same WASM module
- Opens `FileSystemSyncAccessHandle` (available because it's a Dedicated Worker)
- Listens for storage commands (put, checkpoint, etc.)
- Sends back rehydration data on startup

This avoids `SharedArrayBuffer` entirely. No special headers needed. No `Atomics`. The worker is just a persistence backend.

### Storage command protocol (Rust-internal)

Commands from main → worker:

```
StoragePut { key: Vec<u8>, value: Vec<u8> }
StorageDelete { key: Vec<u8> }
Checkpoint                          // flush dirty pages to OPFS
Shutdown                            // close OPFS handle, terminate
```

Messages from worker → main:

```
RehydrationBatch { entries: Vec<(Vec<u8>, Vec<u8>)> }
RehydrationComplete
CheckpointComplete
Error { message: String }
```

These are serialized with postcard (already a dependency) over `postMessage` using `Uint8Array` transfer. This is compact and fast — no JSON, no JS object allocation.

### `ThreadedOpfsStorage` (new Storage impl)

```rust
pub struct ThreadedOpfsStorage {
    /// Plain sorted map serving all synchronous reads. BTreeMap because
    /// index_range needs prefix-based range scans. No page layout, no
    /// superblock — the OPFS B-tree complexity lives only on the worker.
    cache: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Handle to the worker thread for write-behind.
    worker: WorkerHandle,
    /// True if any StoragePut/StorageDelete has been sent since the last
    /// Checkpoint command. Used only to skip no-op checkpoint triggers —
    /// the worker's OpfsBTree tracks per-page dirtiness internally.
    has_pending_writes: bool,
}

impl Storage for ThreadedOpfsStorage {
    fn append_commit(...) -> Result<...> {
        // 1. Write to in-memory map (synchronous, immediate)
        self.cache.insert(key.clone(), value.clone());
        // 2. Queue write to OPFS worker (fire-and-forget postMessage)
        self.worker.send(StoragePut { key, value });
        self.has_pending_writes = true;
        Ok(...)
    }

    fn index_range(...) -> Result<Vec<...>> {
        // BTreeMap::range with prefix bounds — instant
        self.cache.range(prefix_start..prefix_end)
    }
    // ... etc
}
```

**Dirty tracking is split across two layers:**

1. `has_pending_writes` (main thread, `ThreadedOpfsStorage`) — coarse boolean. "Have I sent any `StoragePut`/`StorageDelete` since the last `Checkpoint`?" Used solely to avoid sending pointless `Checkpoint` commands when nothing changed. Reset to `false` after sending `Checkpoint`.

2. Per-page dirty bits (worker thread, `OpfsBTree<OpfsFile>`) — fine-grained. The worker's B-tree internally tracks which pages were modified. When it receives a `Checkpoint` command, it flushes only dirty pages to OPFS via the double-superblock-slot mechanism. This is the existing `OpfsBTree::checkpoint()` logic, unchanged.

The main thread doesn't need to know _which_ pages are dirty — that's the worker's concern. It only needs to know _whether_ a checkpoint is worth requesting.

Key properties:

- **Reads are synchronous and fast** — they hit the in-memory `BTreeMap` only. No page indirection, no file abstraction overhead.
- **Writes are synchronous from the caller's perspective** — the in-memory cache is updated immediately, the OPFS write is fire-and-forget.
- **Checkpoints are periodic** — driven by the scheduler (same as today's WAL flush on lifecycle hints). Skipped when `has_pending_writes` is false.
- **Startup is async** — rehydration must complete before the runtime is ready.

### Startup sequence

```
1. Main thread: create WasmRuntime
2. Main thread: spawn internal worker, send WASM module + db namespace
3. Worker: load WASM, open OPFS FileSystemSyncAccessHandle
4. Worker: read all B-tree entries, send RehydrationBatch messages
5. Worker: send RehydrationComplete
6. Main thread: populate in-memory BTreeMap from rehydration key-value entries
7. Main thread: RuntimeCore is now ready (schema rehydrated, storage populated)
8. Main thread: connect to upstream server, accept queries
```

The `openPersistent()` API stays async — it already is. The difference is it now returns a single runtime on the main thread instead of a runtime + worker bridge.

### Durability guarantees

- **`tier: "worker"` durability** = data is in the in-memory cache AND has been sent to the OPFS worker's inbound queue. The worker will persist it on next checkpoint. This matches today's semantics — the worker's WAL buffer is also not immediately durable on disk.
- **Checkpoint triggers:** lifecycle hints (`visibility-hidden`, `pagehide`, `freeze`), periodic timer, explicit `flush()`.
- **Crash recovery:** Same as today. OPFS B-tree uses double-superblock-slot checkpoints. On restart, rehydration reads the last valid checkpoint.

### What gets deleted

| File                               | LOC  | Purpose                                        | Replacement                                              |
| ---------------------------------- | ---- | ---------------------------------------------- | -------------------------------------------------------- |
| `worker/jazz-worker.ts`            | 655  | Worker-side runtime, server sync, peer routing | Removed — RuntimeCore on main thread handles all of this |
| `runtime/worker-bridge.ts`         | 408  | Main↔worker postMessage bridge                 | Removed — no bridge needed                               |
| `worker/worker-protocol.ts`        | ~100 | Message type definitions                       | Removed                                                  |
| Main-thread MemoryStorage instance | —    | RAM cache of worker state                      | Replaced by `ThreadedOpfsStorage` in-memory cache        |

The server sync connection (`connectStream`, `sendToServer`, reconnect logic) moves into the main-thread runtime's sync transport — which already exists for non-worker deployments.

### Server sync stays in JS

WASM cannot call `fetch()`. HTTP in the browser is a JS-only API. The Rust runtime fires `onSyncMessageToSend()` callbacks, and JS handles the actual HTTP POST to `/sync` and GET streaming from `/events`. This is already how it works today — the change is just that `sync-transport.ts` is consumed by `client.ts` alone instead of being duplicated across `client.ts`, `jazz-worker.ts`, and `worker-bridge.ts`.

The flow after the migration:

```
RuntimeCore (main thread, Rust/WASM)
  │
  ├─ onSyncMessageToSend(callback)
  │    └─ JS callback in client.ts
  │         └─ sendSyncPayload() ──► HTTP POST /sync
  │
  └─ onSyncMessageReceived(payload)
       ▲
       └─ SyncStreamController (JS, client.ts)
            └─ readBinaryFrames() ◄── HTTP GET /events (streaming)
```

`sync-transport.ts` survives but loses its worker/bridge consumers. It becomes a clean, single-purpose HTTP sync client for the main-thread runtime.

### What stays the same

- `RuntimeCore<S, Sch, Sy>` — unchanged. Just gets a new `S = ThreadedOpfsStorage`.
- `Storage` trait — unchanged. Still synchronous, still no Send/Sync.
- `OpfsBTree` / `OpfsBTreeStorage` — unchanged. Still used on the worker thread.
- `SyncFile` trait and `OpfsFile` — unchanged. Still runs in a Dedicated Worker.
- `WasmScheduler` — unchanged. Still uses `spawn_local` on the main thread.
- `sync-transport.ts` — stays in JS. WASM can't call `fetch()`. Used only by `client.ts` (no more worker/bridge duplication).
- Multi-tab leader election — still needed. `FileSystemSyncAccessHandle` is still an exclusive lock held by the worker thread.
- Peer sync routing — handled by RuntimeCore/SyncManager directly, which already supports multiple clients.

### Browser compat and headers

No `SharedArrayBuffer`, no `Atomics`, no COOP/COEP headers needed. The internal worker is a standard Dedicated Worker — the same thing `jazz-worker.ts` already is. The only new capability is spawning a worker from Rust/WASM instead of from JS, which is supported in all modern browsers.

The WASM module needs to be loadable in both the main thread and the worker. This is already the case — `jazz-wasm` is loaded in both contexts today.

### Rehydration cost

Full rehydration on startup reads the entire OPFS B-tree and sends it to the main thread. For a 10MB database with 50k entries, this is:

- Worker: sequential B-tree scan (fast — synchronous OPFS reads)
- Transfer: `postMessage` with `Uint8Array` transfer (zero-copy for the buffer)
- Main thread: insert into `BTreeMap`

This is comparable to today's startup cost (the worker's `openPersistent` already reads the full B-tree to rehydrate the schema manager). The difference is the data also gets sent to the main thread — but this replaces the sync-protocol bootstrap that currently sends catalogue data across anyway.

For very large databases, incremental/lazy rehydration is a future optimization (load hot data first, fill cold data on demand). Not needed for MVP.

## Decisions

1. **Worker spawning from WASM.** Use the same WASM module URL with a query parameter to switch to "storage worker" mode. If that proves impractical (e.g., bundler limitations, service worker URL restrictions), fall back to a blob URL.

2. **Rehydration format.** Key-value entries. Simpler, B-tree-implementation-independent, and lets the main-thread cache use a different data structure if desired.

3. **Write backpressure.** None for MVP. The in-memory cache is authoritative; the OPFS worker is best-effort catch-up.

4. **`noop_main_thread_storage` spec.** Superseded by this spec. The main thread gets real (in-memory) storage that is the single source of truth.

## Non-Goals

- Shared-memory threading (`SharedArrayBuffer` / `Atomics` / `wasm-bindgen-rayon`). Too many deployment constraints (COOP/COEP headers), and unnecessary — we don't need parallel compute, just background I/O.
- Moving query evaluation to a worker. Queries must run on the main thread for synchronous React integration.
- Changing the `Storage` trait to async. The synchronous contract is a feature, not a limitation.

# Storage — Status Quo

The Storage trait is the platform abstraction boundary. Everything above it — [Object Manager](object_manager.md), [Query Manager](query_manager.md), [Sync Manager](sync_manager.md) — is platform-agnostic Rust. Everything below it is platform-specific: file I/O on native, OPFS in the browser.

The critical design choice is that Storage is **synchronous**. The query engine needs immediate answers — when a query asks "what rows match this filter?", the index lookup returns right now, not via a callback. This eliminates an entire class of complexity: no Loading states, no pending queues, no "data might not be ready yet" checks. The insight that made this possible is OPFS's `FileSystemSyncAccessHandle`, which provides synchronous file I/O in Dedicated Workers.

## Storage Trait

Synchronous, single-threaded interface (no Send + Sync bounds). All methods return results immediately — no Loading states, no async gaps.

Operations:

- **Objects**: `create_object()`, `load_object_metadata()`, `load_branch()`, `append_commit()`, `delete_commit()`, `set_branch_tails()`
- **Indices**: `index_insert()`, `index_remove()`, `index_lookup()`, `index_range()`, `index_scan_all()`

> `crates/groove/src/storage/mod.rs:67-195` (trait definition)

## Implementations

### MemoryStorage

HashMap-backed, used for tests and the browser main thread (acts as cache of worker state).

> `crates/groove/src/storage/mod.rs:200+`

### BfTreeStorage

legacy storage is our own B-tree key-value store, purpose-built for this use case. It supports both native (file-backed) and WASM (OPFS-backed) storage.

The key insight is using composite keys so that B-tree range scans naturally give us index lookups:

```
idx:{table}:{column}:{branch}:{encoded_value}:{row_id}
```

A range scan over a prefix like `idx:todos:done:main:` returns all row IDs in the `done` index for the `todos` table on branch `main`. No separate index data structure needed — the B-tree IS the index.

> `crates/groove/src/storage/legacy storage.rs`
> `crates/legacy storage/` (underlying KV store, with WASM feature)

## Deployment Topology

The browser case is the interesting one. We can't block the main thread on storage I/O, but we need synchronous storage for the query engine. The solution: run the persistent groove instance in a Dedicated Worker (where `SyncAccessHandle` gives us sync I/O), and keep a lightweight MemoryStorage cache on the main thread for instant reads.

### Browser

```
┌──────────────────────────────────────────────────────┐
│ MAIN THREAD: Groove (MemoryStorage)                   │
│  - All operations sync, in-memory cache               │
└──────────────────┬───────────────────────────────────┘
              postMessage (sync protocol)
┌──────────────────┴───────────────────────────────────┐
│ DEDICATED WORKER: Groove (BfTreeStorage/OPFS)         │
│  - Durable via FileSystemSyncAccessHandle             │
│  - Upstream server connection                         │
└──────────────────┬───────────────────────────────────┘
                   │ HTTP/SSE
                   ▼
           Edge Server (Groove + BfTreeStorage)
```

OPFS provides synchronous I/O via `FileSystemSyncAccessHandle` in Dedicated Workers — no need for async storage abstractions.

> `crates/groove-wasm/src/runtime.rs` (WasmRuntime with BfTreeStorage)
> `packages/jazz-ts/src/worker/groove-worker.ts` (worker entry point)

### Native (Node.js / Rust)

Single process, no worker needed. BfTreeStorage backed by regular files.

> `crates/groove-tokio/src/lib.rs` (TokioRuntime with BfTreeStorage)

## Platform Bindings

### groove-napi (Node.js)

NAPI bindings exposing RuntimeCore to Node.js via TokioRuntime.

> `crates/groove-napi/`

### groove-wasm (Browser)

WASM bindings exposing RuntimeCore via WasmRuntime. WasmScheduler uses `spawn_local`; JsSyncSender serializes messages to JSON for JS callbacks.

> `crates/groove-wasm/`

## Design Decisions

| Decision            | Choice                                        | Rationale                                                               |
| ------------------- | --------------------------------------------- | ----------------------------------------------------------------------- |
| Index encoding      | Composite keys in legacy storage              | Range queries give index scans naturally                                |
| Durability default  | Fire-and-forget                               | Optimistic local-first; `_persisted()` variants for explicit durability |
| Native architecture | Single process                                | No worker overhead needed                                               |
| Tab coordination    | Single tab owns OPFS (leader election future) | `SyncAccessHandle` is an exclusive lock                                 |

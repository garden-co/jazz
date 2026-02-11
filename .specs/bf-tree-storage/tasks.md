# Implementation Tasks

## Phase 1: Extend `cojson-core-wasm` with bf-tree bindings

- [x] 1. Add `bf-tree = { path = "../bf-tree-web" }` dependency to `crates/cojson-core-wasm/Cargo.toml`
- [x] 2. Create `crates/cojson-core-wasm/src/storage/mod.rs` with `BfTreeStore` wasm_bindgen struct wrapping bf-tree's `insert`, `read`, `delete` methods
- [x] 3. Add `open_bftree_opfs(db_name, cache_size_bytes)` async factory function (wasm_bindgen) that initializes OPFS VFS and creates a configured BfTree
- [x] 4. Add `create_bftree_memory(cache_size_bytes)` sync factory function (wasm_bindgen) for in-memory trees (testing)
- [x] 5. Register `pub mod storage;` in `crates/cojson-core-wasm/src/lib.rs`
- [x] 6. Verify WASM build: run `pnpm build:wasm` in `crates/cojson-core-wasm` and confirm the new `BfTreeStore`, `open_bftree_opfs`, `create_bftree_memory` appear in the generated `.d.ts`

## Phase 2: Prefix Scan Support

- [x] 7. Add a `scan` method to `BfTreeStore` in `crates/cojson-core-wasm/src/storage/mod.rs` that wraps bf-tree's `ScanIter` — accepts a prefix byte slice and limit, returns an array of `[key, value]` pairs as `JsValue`
- [x] 8. Rebuild WASM and verify `scan` appears in the generated TypeScript declarations

## Phase 3: Package Scaffold

- [x] 9. Create `packages/cojson-storage-bftree/` directory with `package.json` (name: `cojson-storage-bftree`, dependencies: `cojson: workspace:*`, `cojson-core-wasm: workspace:*`)
- [x] 10. Create `tsconfig.json` (modeled on `cojson-storage-indexeddb/tsconfig.json`, with `lib: ["ESNext", "DOM", "WebWorker"]`)
- [x] 11. Register the new package in the pnpm workspace (`pnpm-workspace.yaml`)

## Phase 4: Worker Protocol & Infrastructure

- [x] 12. Create `src/protocol.ts` — shared type definitions: `WorkerRequest`, `WorkerResponse`, `WorkerInitRequest`, `WorkerInitResponse`, and the method name union type
- [x] 13. Create `src/keys.ts` — key encoding utilities (`Keys.coValue`, `Keys.session`, `Keys.transaction`, `Keys.signature`, `Keys.deleted`, `Keys.unsynced` and their prefix variants, plus `padIdx`)

## Phase 5: Worker-Side Implementation

- [x] 14. Create `src/workerBackend.ts` — `BfTreeWorkerBackend` class with:
    - `BfTreeStore` wrappers: `put` / `get` / `del` / `scanByPrefix` (TextEncoder/TextDecoder + JSON serialization)
    - Row ID mapping (coValueId ↔ rowId, sessionKey ↔ rowId) with in-memory counters
    - `dispatch(method, args)` router
    - All `DBClientInterfaceAsync` method implementations: `getCoValue`, `upsertCoValue`, `getCoValueSessions`, `getNewTransactionInSession`, `getSignatures`, `getAllCoValuesWaitingForDelete`, `trackCoValuesSyncState`, `getUnsyncedCoValueIDs`, `stopTrackingSyncState`, `eraseCoValueButKeepTombstone`, `getCoValueKnownState`
    - All `DBTransactionInterfaceAsync` method implementations: `getSingleCoValueSession`, `markCoValueAsDeleted`, `addSessionUpdate`, `addTransaction`, `addSignatureAfter`, `deleteCoValueContent`
- [x] 15. Create `src/worker.ts` — Worker entry point that imports `cojson-core-wasm`, calls `initialize()` + `open_bftree_opfs()`, then dispatches `WorkerRequest` messages to `BfTreeWorkerBackend`

## Phase 6: Main-Thread Client

- [x] 16. Create `src/client.ts` — `BfTreeClient` implementing `DBClientInterfaceAsync` as an RPC proxy:
    - Constructor takes a `Worker` instance, sets up `onmessage` handler to resolve pending Promises by `reqId`
    - `call<T>(method, args): Promise<T>` helper that sends `WorkerRequest` and returns a Promise
    - All `DBClientInterfaceAsync` methods delegating to `call()`
    - `transaction()` method creating a `txProxy` object that forwards each sub-operation as a `tx.*` worker call
- [x] 17. Create `src/index.ts` — `getBfTreeStorage()` factory function:
    - Creates `new Worker(new URL("./worker.js", import.meta.url), { type: "module" })`
    - Sends `init` message and awaits `ready` response
    - Returns `new StorageApiAsync(new BfTreeClient(worker))`

## Phase 7: Testing

- [x] 18. Create `src/tests/bftree.storage.test.ts` — integration tests modeled on `cojson-storage-indexeddb` test suite: store/load round-trip, multi-session content, deletion + tombstone preservation, sync state tracking, sync resumption, account load round-trip (11 tests)
- [x] 19. Add vitest project configuration (`vitest.config.ts` with Playwright browser provider)

## Phase 8: Build Integration

- [x] 20. Add `build` script to `packages/cojson-storage-bftree/package.json` (TypeScript compilation) and register in Turbo build graph with dependency on `cojson-core-wasm` WASM build

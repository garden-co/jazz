# Implementation Tasks

## Phase 1: Enhanced Worker Backend

- [ ] 1. Add `storeContent(msg, deletedCoValues)` method to `BfTreeWorkerBackend` in `src/workerBackend.ts` — performs upsert + session updates + transaction writes + signature writes in a single synchronous call, returns `{ knownState, storedCoValueRowID }`. Port the store logic from `StorageApiAsync.storeSingle()` and `putNewTxs()`.
- [ ] 2. Add `loadContent(id)` method to `BfTreeWorkerBackend` — loads CoValue, all sessions, transactions, and signatures, assembles `NewContentMessage` objects (with streaming chunks for multi-signature CoValues), returns `{ messages, found }`. Port the load logic from `StorageApiAsync.loadCoValue()`.
- [ ] 3. Add `enableDeletedCoValuesErasure()` and `eraseAllDeletedCoValues()` methods to `BfTreeWorkerBackend` — manage the `DeletedCoValuesEraserScheduler` inside the worker.
- [ ] 4. Import required helpers into `workerBackend.ts`: `createContentMessage`, `exceedsRecommendedSize`, `collectNewTxs`, `getNewTransactionsSize`, `getDependedOnCoValues`, `isDeleteSessionID` from `cojson` internals.

## Phase 2: New Protocol Types

- [ ] 5. Rewrite `src/protocol.ts` with discriminated union types for `WorkerRequest` and `WorkerResponse` aligned with `StorageAPI`-level operations (load, store, loadKnownState, eraseAllDeletedCoValues, getUnsyncedCoValueIDs, close, plus fire-and-forget messages).

## Phase 3: Main-Thread Proxy

- [ ] 6. Create `src/proxy.ts` — `BfTreeStorageProxy` class implementing `StorageAPI`:
    - `StorageKnownState` cache for synchronous `getKnownState()`
    - `StoreQueue` for serialization + correction handling
    - `inMemoryCoValues` set and `deletedValues` set
    - `pendingKnownStateLoads` deduplication map
    - `load()`: sends request to worker, handles streaming `load:data`/`load:done` responses
    - `store()`: queues via `StoreQueue`, sends to worker, handles corrections locally
    - `waitForSync()`: delegates to `StorageKnownState.waitForSync()`
    - Fire-and-forget methods: `markDeleteAsValid`, `enableDeletedCoValuesErasure`, `trackCoValuesSyncState`, `stopTrackingSyncState`, `onCoValueUnmounted`
    - Request/response methods: `eraseAllDeletedCoValues`, `getUnsyncedCoValueIDs`, `loadKnownState`, `close`

## Phase 4: Worker Entry Point

- [ ] 7. Rewrite `src/worker.ts` to dispatch `StorageAPI`-level messages directly to `BfTreeWorkerBackend`'s new high-level methods (`storeContent`, `loadContent`, etc.) instead of the old `dispatch(method, args)` routing.

## Phase 5: Factory & Exports

- [ ] 8. Update `src/index.ts` — `getBfTreeStorage()` returns `new BfTreeStorageProxy(worker)` directly instead of `new StorageApiAsync(new BfTreeClient(worker))`. Remove `BfTreeClient` and `StorageApiAsync` imports.

## Phase 6: Remove Obsolete Code

- [ ] 9. Delete `src/client.ts` (`BfTreeClient` implementing `DBClientInterfaceAsync` — fully replaced by `BfTreeStorageProxy`).
- [ ] 10. Remove the old `dispatch(method, args)` method and its `DBClientInterfaceAsync`/`DBTransactionInterfaceAsync` routing table from `BfTreeWorkerBackend` (the individual low-level methods remain, but the string-based dispatch is no longer needed).

## Phase 7: Test Adaptation

- [ ] 11. Update `src/tests/testUtils.ts` — replace `DirectBfTreeClient` (the test `DBClientInterfaceAsync` adapter) with a `DirectBfTreeStorageProxy` that implements `StorageAPI` by calling `BfTreeWorkerBackend.storeContent()` / `loadContent()` directly. Update `createStorageFromBackend()` to return the new proxy.
- [ ] 12. Update `src/tests/bftree.storage.test.ts` — ensure all existing tests pass with the new architecture (no test logic changes expected, only factory/helper changes).

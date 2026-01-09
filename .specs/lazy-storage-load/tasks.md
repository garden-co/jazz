# Implementation Tasks

## Phase 1: Storage Layer (US-3)

- [ ] **Task 1**: Add `getCoValueKnownState` method to DB client interfaces
  - Add `getCoValueKnownState(coValueId: string): CoValueKnownState | undefined` to `DBClientInterfaceSync` in `packages/cojson/src/storage/types.ts`
  - Add `getCoValueKnownState(coValueId: string): Promise<CoValueKnownState | undefined>` to `DBClientInterfaceAsync` in `packages/cojson/src/storage/types.ts`

- [ ] **Task 2**: Implement `getCoValueKnownState` in SQLite sync client
  - Implement method in `packages/cojson/src/storage/sqlite/client.ts`
  - Query `coValues` table to check existence and get `rowID`
  - Query `sessions` table to get `sessionID` and `lastIdx` for all sessions
  - Return `CoValueKnownState` with `header: true` and session counters, or `undefined` if not found

- [ ] **Task 3**: Implement `getCoValueKnownState` in SQLite async client
  - Implement async method in `packages/cojson/src/storage/sqliteAsync/client.ts`
  - Same logic as Task 2 but with `async/await`

- [ ] **Task 4**: Implement `getCoValueKnownState` in IndexedDB client
  - Implement method in `packages/cojson-storage-indexeddb/src/idbClient.ts`
  - Query coValues and sessions stores to build knownState
  - Return `undefined` if CoValue doesn't exist

- [ ] **Task 5**: Add `loadKnownState` method to `StorageAPI` interface
  - Add `loadKnownState(id: string, callback: (knownState: CoValueKnownState | undefined) => void): void` to `StorageAPI` interface in `packages/cojson/src/storage/types.ts`

- [ ] **Task 6**: Implement `loadKnownState` in `StorageApiSync`
  - Implement method in `packages/cojson/src/storage/storageSync.ts`
  - Check in-memory `knownStates` cache first (only if `header: true`)
  - If not cached, call `dbClient.getCoValueKnownState()`
  - Cache the result if found
  - Call callback with result

- [ ] **Task 7**: Implement `loadKnownState` in `StorageApiAsync`
  - Implement async method in `packages/cojson/src/storage/storageAsync.ts`
  - Same logic as Task 6 but with `async/await`

## Phase 2: CoValueCore Methods (US-1, US-3)

- [ ] **Task 8**: Add `peerHasAllContent` helper function
  - Add function to `packages/cojson/src/knownState.ts`
  - Compare storage knownState with peer knownState
  - Return `true` if peer has header (when storage has it) AND peer has >= transactions for all sessions

- [ ] **Task 9**: Implement `lazyLoadFromStorage` method in `CoValueCore`
  - Add method to `packages/cojson/src/coValueCore/coValueCore.ts`
  - Handle case: no storage → return `undefined`
  - Handle case: already available in memory → return current `knownState()`
  - Handle case: loading state is "pending" → subscribe and wait for result
  - Handle case: loading state is "available" → return `knownState()`
  - Handle case: loading state is "unavailable" or "errored" → return `undefined`
  - Handle case: loading state is "unknown" → call `storage.loadKnownState()`

- [ ] **Task 10**: Implement `lazyLoad` method in `CoValueCore`
  - Add method to `packages/cojson/src/coValueCore/coValueCore.ts`
  - Accept `peerKnownState` and callbacks object `{ onNeedsContent, onUpToDate, onNotFound }`
  - If already available → call `onNeedsContent`
  - Call `lazyLoadFromStorage` to get storage knownState
  - If not found → call `onNotFound`
  - If found and `peerHasAllContent` returns true → call `onUpToDate(storageKnownState)`
  - If found and peer needs content → call `loadFromStorage`, then `onNeedsContent`

## Phase 3: SyncManager Integration (US-1, US-2)

- [ ] **Task 11**: Modify `handleLoad` to use `lazyLoad`
  - Update `handleLoad` method in `packages/cojson/src/sync.ts`
  - Keep existing fast path for CoValue already in memory
  - Replace storage/peer loading logic with `coValue.lazyLoad()`
  - Map callbacks to appropriate responses:
    - `onNeedsContent` → `sendNewContent()`
    - `onUpToDate` → send "known" message with storageKnownState
    - `onNotFound` → `loadFromPeersAndRespond()`

- [ ] **Task 12**: Add `loadFromPeersAndRespond` helper method
  - Add private method to `SyncManager` in `packages/cojson/src/sync.ts`
  - Get server peers, call `loadFromPeers`
  - Wait for result, then `sendNewContent` or `handleLoadNotFound`

- [ ] **Task 13**: Add `handleLoadNotFound` helper method
  - Add private method to `SyncManager` in `packages/cojson/src/sync.ts`
  - Send "known" message with `header: false` and empty sessions

- [ ] **Task 14**: Modify `handleNewContent` to use `lazyLoadFromStorage`
  - Update `handleNewContent` method in `packages/cojson/src/sync.ts`
  - In the section handling CoValue not in memory with no header:
  - Replace `storage.getKnownState()` check with `coValue.lazyLoadFromStorage()`
  - If storageKnownState found → load from storage, then re-call `handleNewContent`
  - If not found → request full content from peer

## Phase 4: Testing

- [ ] **Task 15**: Write unit tests for `getCoValueKnownState` (DB clients)
  - Test returns correct knownState structure for existing CoValue
  - Test returns `undefined` for non-existent CoValue
  - Test handles CoValue with no sessions (header only)
  - Test handles CoValue with multiple sessions
  - Add tests to `packages/cojson/src/tests/` for SQLite clients

- [ ] **Task 16**: Write unit tests for `loadKnownState` (StorageAPI)
  - Test uses cache when knownState is cached with `header: true`
  - Test queries DB when not cached
  - Test caches result after DB query
  - Test returns `undefined` for non-existent CoValue
  - Add tests to `packages/cojson/src/tests/`

- [ ] **Task 17**: Write unit tests for `peerHasAllContent`
  - Test returns `true` when peer has everything
  - Test returns `false` when peer is missing header
  - Test returns `false` when peer is missing sessions
  - Test returns `false` when peer has fewer transactions in a session
  - Test returns `true` when peer has MORE transactions (peer ahead)
  - Test returns `false` when peerKnownState is `undefined`
  - Add tests to `packages/cojson/src/tests/`

- [ ] **Task 18**: Write unit tests for `lazyLoadFromStorage`
  - Test returns `undefined` when no storage
  - Test returns knownState when CoValue is already in memory
  - Test waits for pending load if already in progress
  - Test returns based on loading state (available/unavailable/errored)
  - Test calls `storage.loadKnownState` when state is unknown
  - Add tests to `packages/cojson/src/tests/coValueCore.test.ts`

- [ ] **Task 19**: Write unit tests for `lazyLoad`
  - Test calls `onNeedsContent` when CoValue already in memory
  - Test calls `onNotFound` when not in storage
  - Test calls `onUpToDate` when peer has all content
  - Test calls `onNeedsContent` after full load when peer needs content
  - Add tests to `packages/cojson/src/tests/coValueCore.test.ts`

- [ ] **Task 20**: Write integration tests for lazy load flow
  - Test `handleLoad` skips full load when peer has all content
  - Test `handleLoad` does full load when peer needs content
  - Test `handleLoad` falls back to peers when not in storage
  - Test `handleNewContent` loads from storage for garbage-collected CoValues
  - Test full flow: verify no transaction table queries when peer is up-to-date
  - Add tests to `packages/cojson/src/tests/sync.load.test.ts`

- [ ] **Task 21**: Write performance tests
  - Benchmark `loadKnownState` vs `load` for CoValue with many transactions
  - Measure memory usage difference
  - Test with high volume of load requests
  - Add tests to `packages/cojson/src/tests/` or `bench/`

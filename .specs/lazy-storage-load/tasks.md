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
  - Implement method in `packages/cojson/src/storage/storageAsync.ts`
  - Check in-memory `knownStates` cache first
  - If not cached, check for pending load (deduplication)
  - If pending load exists, attach callback to existing promise
  - If no pending load, start new load and track in `pendingKnownStateLoads` map
  - Cache the result if found, then remove from pending map

## Phase 2: CoValueCore Methods (US-1, US-3)

- [ ] **Task 8**: Add `peerHasAllContent` helper function
  - Add function to `packages/cojson/src/knownState.ts`
  - Compare storage knownState with peer knownState
  - Return `true` if peer has header (when storage has it) AND peer has >= transactions for all sessions

- [ ] **Task 9**: Implement `getKnownStateFromStorage` method in `CoValueCore`
  - Add method to `packages/cojson/src/coValueCore/coValueCore.ts`
  - Handle case: no storage → return `undefined`
  - Handle case: already available in memory → return current `knownState()`
  - Otherwise → delegate to `storage.loadKnownState()` (caching handled at storage level)

## Phase 3: SyncManager Integration (US-1, US-2)

- [ ] **Task 10**: Modify `handleLoad` to use lazy storage loading
  - Update `handleLoad` method in `packages/cojson/src/sync.ts`
  - Keep existing fast path for CoValue already in memory
  - Use `coValue.getKnownStateFromStorage()` to check storage before full load
  - If not found in storage → call `loadFromPeersAndRespond()`
  - If found and `peerHasAllContent` returns true → send "known" message with storageKnownState
  - If found and peer needs content → call `loadFromStorage`, then `sendNewContent()`

- [ ] **Task 11**: Add `loadFromPeersAndRespond` helper method
  - Add private method to `SyncManager` in `packages/cojson/src/sync.ts`
  - Get server peers, call `loadFromPeers`
  - Wait for result, then `sendNewContent` or `handleLoadNotFound`

- [ ] **Task 12**: Add `handleLoadNotFound` helper method
  - Add private method to `SyncManager` in `packages/cojson/src/sync.ts`
  - Send "known" message with `header: false` and empty sessions

- [ ] **Task 13**: Modify `handleNewContent` to use `getKnownStateFromStorage`
  - Update `handleNewContent` method in `packages/cojson/src/sync.ts`
  - In the section handling CoValue not in memory with no header:
  - Replace `storage.getKnownState()` check with `coValue.getKnownStateFromStorage()`
  - If storageKnownState found → load from storage, then re-call `handleNewContent`
  - If not found → request full content from peer

## Phase 4: Testing

- [ ] **Task 14**: Write unit tests for `getCoValueKnownState` (DB clients)
  - Test returns correct knownState structure for existing CoValue
  - Test returns `undefined` for non-existent CoValue
  - Test handles CoValue with no sessions (header only)
  - Test handles CoValue with multiple sessions
  - Add tests to `packages/cojson/src/tests/` for SQLite clients

- [ ] **Task 15**: Write unit tests for `loadKnownState` (StorageAPI)
  - Test uses cache when knownState is cached with `header: true`
  - Test queries DB when not cached
  - Test caches result after DB query
  - Test returns `undefined` for non-existent CoValue
  - Add tests to `packages/cojson/src/tests/`

- [ ] **Task 16**: Write unit tests for `peerHasAllContent`
  - Test returns `true` when peer has everything
  - Test returns `false` when peer is missing header
  - Test returns `false` when peer is missing sessions
  - Test returns `false` when peer has fewer transactions in a session
  - Test returns `true` when peer has MORE transactions (peer ahead)
  - Test returns `false` when peerKnownState is `undefined`
  - Add tests to `packages/cojson/src/tests/`

- [ ] **Task 17**: Write unit tests for `getKnownStateFromStorage`
  - Test returns `undefined` when no storage
  - Test returns knownState when CoValue is already in memory
  - Test calls `storage.loadKnownState` when CoValue not in memory
  - Test returns `undefined` when storage does not have the CoValue
  - Add tests to `packages/cojson/src/tests/coValueCore.lazyLoading.test.ts`

- [ ] **Task 18**: Write integration tests for lazy load flow
  - Test `handleLoad` skips full load when peer has all content
  - Test `handleLoad` does full load when peer needs content
  - Test `handleLoad` falls back to peers when not in storage
  - Test `handleNewContent` loads from storage for garbage-collected CoValues
  - Test full flow: verify no transaction table queries when peer is up-to-date
  - Add tests to `packages/cojson/src/tests/sync.load.test.ts`

- [ ] **Task 19**: Write performance tests
  - Benchmark `loadKnownState` vs `load` for CoValue with many transactions
  - Measure memory usage difference
  - Test with high volume of load requests
  - Add tests to `packages/cojson/src/tests/` or `bench/`

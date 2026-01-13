# Implementation Tasks

## Phase 1: Storage Layer (US-3)

- [x] **Task 1**: Add `getCoValueKnownState` method to DB client interfaces
  - Add `getCoValueKnownState(coValueId: string): CoValueKnownState | undefined` to `DBClientInterfaceSync` in `packages/cojson/src/storage/types.ts`
  - Add `getCoValueKnownState(coValueId: string): Promise<CoValueKnownState | undefined>` to `DBClientInterfaceAsync` in `packages/cojson/src/storage/types.ts`

- [x] **Task 2**: Implement `getCoValueKnownState` in SQLite sync client
  - Implement method in `packages/cojson/src/storage/sqlite/client.ts`
  - Query `coValues` table to check existence and get `rowID`
  - Query `sessions` table to get `sessionID` and `lastIdx` for all sessions
  - Return `CoValueKnownState` with `header: true` and session counters, or `undefined` if not found

- [x] **Task 3**: Implement `getCoValueKnownState` in SQLite async client
  - Implement async method in `packages/cojson/src/storage/sqliteAsync/client.ts`
  - Same logic as Task 2 but with `async/await`

- [x] **Task 4**: Implement `getCoValueKnownState` in IndexedDB client
  - Implement method in `packages/cojson-storage-indexeddb/src/idbClient.ts`
  - Query coValues and sessions stores to build knownState
  - Return `undefined` if CoValue doesn't exist

- [x] **Task 5**: Add `loadKnownState` method to `StorageAPI` interface
  - Add `loadKnownState(id: string, callback: (knownState: CoValueKnownState | undefined) => void): void` to `StorageAPI` interface in `packages/cojson/src/storage/types.ts`

- [x] **Task 6**: Implement `loadKnownState` in `StorageApiSync`
  - Implement method in `packages/cojson/src/storage/storageSync.ts`
  - Check in-memory `knownStates` cache first (only if `header: true`)
  - If not cached, call `dbClient.getCoValueKnownState()`
  - Cache the result if found
  - Call callback with result

- [x] **Task 7**: Implement `loadKnownState` in `StorageApiAsync`
  - Implement method in `packages/cojson/src/storage/storageAsync.ts`
  - Check in-memory `knownStates` cache first
  - If not cached, check for pending load (deduplication)
  - If pending load exists, attach callback to existing promise
  - If no pending load, start new load and track in `pendingKnownStateLoads` map
  - Cache the result if found
  - **Error handling contract**: on DB error, log warning and behave like "not found" (`undefined`) so callers can fall back
  - **Dedup safety**: remove from pending map in a `finally` so it is cleared on both success and failure
  - Ensure the callback is always called (even on failure)

## Phase 2: CoValueCore Methods (US-1, US-3)

- [x] **Task 8**: Add `peerHasAllContent` helper function
  - Add function to `packages/cojson/src/knownState.ts`
  - Compare storage knownState with peer knownState
  - Return `true` if peer has header (when storage has it) AND peer has >= transactions for all sessions

- [x] **Task 9**: Implement `getKnownStateFromStorage` method in `CoValueCore`
  - Add method to `packages/cojson/src/coValueCore/coValueCore.ts`
  - Handle case: no storage → return `undefined`
  - Handle case: already available in memory → return current `knownState()`
  - Otherwise → delegate to `storage.loadKnownState()` (caching handled at storage level)

## Phase 3: SyncManager Integration (US-1, US-2)

- [x] **Task 10**: Modify `handleLoad` to use lazy storage loading
  - Update `handleLoad` method in `packages/cojson/src/sync.ts`
  - Keep existing fast path for CoValue already in memory
  - Use `coValue.getKnownStateFromStorage()` to check storage before full load
  - **Race safety**: inside the `getKnownStateFromStorage` callback, re-check `coValue.isAvailable()` and send new content if it became available while waiting
  - If not found in storage → call `loadFromPeersAndRespond()`
  - If found and `peerHasAllContent` returns true → send "known" message with storageKnownState
  - If found and peer needs content → call `loadFromStorage`, then `sendNewContent()`

- [x] **Task 11**: Add `loadFromPeersAndRespond` helper method
  - Add private method to `SyncManager` in `packages/cojson/src/sync.ts`
  - Get server peers, call `loadFromPeers`
  - Wait for result, then `sendNewContent` or `handleLoadNotFound`

- [x] **Task 12**: Add `handleLoadNotFound` helper method
  - Add private method to `SyncManager` in `packages/cojson/src/sync.ts`
  - Send "known" message with `header: false` and empty sessions

- [x] **Task 13**: Modify `handleNewContent` to use `loadFromStorage`
  - Update `handleNewContent` method in `packages/cojson/src/sync.ts`
  - In the section handling CoValue not in memory with no header:
  - Try to load from storage using `coValue.loadFromStorage()`
  - If found → re-call `handleNewContent`
  - If not found → request full content from peer

## Phase 4: Testing

- [x] **Task 14**: Write unit tests for `getCoValueKnownState` (DB clients)
  - Test returns correct knownState structure for existing CoValue
  - Test returns `undefined` for non-existent CoValue
  - Test handles CoValue with no sessions (header only)
  - Test handles CoValue with multiple sessions
  - Add tests to `packages/cojson/src/tests/StorageApiSync.test.ts`

- [x] **Task 15**: Write unit tests for `loadKnownState` (StorageAPI)
  - Test uses cache when knownState is cached with `header: true`
  - Test queries DB when not cached
  - Test caches result after DB query
  - Test returns `undefined` for non-existent CoValue
  - Test async error path: DB rejects → callback is called with `undefined` and pending dedupe entry is cleared
  - Add tests to `packages/cojson/src/tests/StorageApiAsync.test.ts`

- [x] **Task 16**: Write unit tests for `peerHasAllContent`
  - Test returns `true` when peer has everything
  - Test returns `false` when peer is missing header
  - Test returns `false` when peer is missing sessions
  - Test returns `false` when peer has fewer transactions in a session
  - Test returns `true` when peer has MORE transactions (peer ahead)
  - Test returns `false` when peerKnownState is `undefined`
  - Add tests to `packages/cojson/src/tests/coValueCore.lazyLoading.test.ts`

- [x] **Task 17**: Write unit tests for `getKnownStateFromStorage`
  - Test returns `undefined` when no storage
  - Test returns knownState when CoValue is already in memory
  - Test calls `storage.loadKnownState` when CoValue not in memory
  - Test returns `undefined` when storage does not have the CoValue
  - Add tests to `packages/cojson/src/tests/coValueCore.lazyLoading.test.ts`

- [x] **Task 18**: Write integration tests for lazy load flow
  - Test `handleLoad` skips full load when peer has all content
  - Test `handleLoad` does full load when peer needs content
  - Test `handleLoad` falls back to peers when not in storage
  - Test `handleNewContent` loads from storage for garbage-collected CoValues
  - Test server responds with known state when peer already has all content
  - Add tests to `packages/cojson/src/tests/sync.load.test.ts`


# Implementation Tasks

## Core Infrastructure

- [x] **Task 1**: Extend `StorageAPI` interface with unsynced CoValues tracking methods (US1)
  - Add `trackCoValueSyncState(id: RawCoID, peerId: PeerID, synced: boolean): void` to `StorageAPI` interface in `packages/cojson/src/storage/types.ts`
  - Add `getUnsyncedCoValueIDs(callback: (data: RawCoID[]) => void)` to `StorageAPI` interface
  - Add `stopTrackingSyncState(id: RawCoID): void` to `StorageAPI` interface

- [x] **Task 2**: Implement `trackCoValueSyncState`, `getUnsyncedCoValueIDs`, and `stopTrackingSyncState` for IndexedDB storage (US1)
  - Add new object store `"unsyncedCoValues"` in IndexedDB schema (upgrade version)
  - Implement `trackCoValueSyncState` in `StorageApiAsync` to upsert/delete records
  - Implement `getUnsyncedCoValueIDs` to query all unsynced CoValue IDs
  - Implement `stopTrackingSyncState` to delete all records for a CoValue ID
  - Update `packages/cojson-storage-indexeddb/src/idbNode.ts` for schema migration

- [x] **Task 3**: Implement `trackCoValueSyncState`, `getUnsyncedCoValueIDs`, and `stopTrackingSyncState` for SQLite storage (US1)
  - Add `unsynced_covalues` table to SQLite schema
  - Implement `trackCoValueSyncState` in `StorageApiAsync` and `StorageApiSync` for SQLite
  - Implement `getUnsyncedCoValueIDs` in `StorageApiAsync` and `StorageApiSync` for SQLite
  - Implement `stopTrackingSyncState` in `StorageApiAsync` and `StorageApiSync` for SQLite
  - Update SQLite client implementations in `packages/cojson/src/storage/sqlite/` and `packages/cojson/src/storage/sqliteAsync/`

- [x] **Task 4**: Create `UnsyncedCoValuesTracker` class (US1, US3, US4)
  - Create `packages/cojson/src/sync/UnsyncedCoValuesTracker.ts`
  - Implement in-memory `Map<RawCoID, Set<PeerID>>` for tracking unsynced CoValues per peer
  - Implement `add(id, peerId)`, `remove(id, peerId)`, `getAll()`, `isAllSynced()` methods
  - Implement batched/async persistence using `StorageAPI.trackCoValueSyncState`
  - Implement `subscribe(id, listener)` for per-CoValue subscriptions
  - Implement `subscribe(listener)` for global "all synced" subscriptions
  - Handle storage errors gracefully (fallback to in-memory only)

## Integration with Sync System

- [x] **Task 5**: Integrate `UnsyncedCoValuesTracker` into `SyncManager` (US1)
  - Add `unsyncedTracker` property to `SyncManager` class
  - Initialize tracker in `SyncManager` constructor: `new UnsyncedCoValuesTracker(local.storage, this)`
  - Create `trackSyncState(coValueId)` helper method that:
    - Iterates through all persistent server peers using `getPersistentServerPeers()`
    - Calls `unsyncedTracker.add(coValueId, peer.id)` for each peer
    - Subscribes to `syncState.subscribeToPeerUpdates()` for each peer to remove from tracker when synced
    - Handles unsubscribe cleanup when CoValue becomes synced to a peer
  - Update `syncContent()` method to call `trackSyncState(coValue.id)` after storing content
  - Update `handleNewContent()` method to call `trackSyncState(coValue.id)` when receiving content from client peers

- [ ] **Task 6**: Implement `resumeUnsyncedCoValues()` method (US2)
  - Add `async resumeUnsyncedCoValues()` method to `SyncManager`
  - Load persisted unsynced CoValue IDs from storage using `storage.getUnsyncedCoValueIDs()`
  - Process CoValues in batches (e.g., 10 at a time) to avoid blocking
  - For each unsynced CoValue ID, load the CoValue using `local.loadCoValueCore()`
  - If CoValue loads successfully, call `trackSyncState()` to resume tracking
  - If CoValue fails to load or is unavailable, call `storage.stopTrackingSyncState()` to clean up
  - Use `setTimeout(processBatch, 0)` to yield control between batches
  - Call `resumeUnsyncedCoValues()` in `startPeerReconciliation()` method
  - Ensure it runs asynchronously and doesn't block initialization

## Subscription APIs

- [ ] **Task 7**: Implement `CoValueCore.subscribeToSyncState()` (US3)
  - Add `subscribeToSyncState(listener)` method to `CoValueCore` class
  - Use `syncManager.unsyncedTracker.subscribe(this.id, listener)` internally
  - Call listener immediately with current state (check if CoValue ID is in `unsyncedTracker.getAll()`)
  - Return unsubscribe function
  - Update `packages/cojson/src/coValueCore/coValueCore.ts`

- [ ] **Task 8**: Implement `SyncManager.subscribeToSyncState()` (US4)
  - Add `subscribeToSyncState(listener)` method to `SyncManager` class
  - Use `unsyncedTracker.subscribe(listener)` internally
  - Call listener immediately with current state (`unsyncedTracker.isAllSynced()`)
  - Return unsubscribe function
  - Update `packages/cojson/src/sync.ts`

## Refactored waitForSync

- [ ] **Task 9**: Refactor `CoValueCore.waitForSync()` to use tracker (US5)
  - Replace current subscription-based implementation
  - Use `syncManager.unsyncedTracker.subscribe(this.id, ...)` instead
  - Maintain backward compatibility with existing API signature
  - Support timeout option if needed
  - Update `packages/cojson/src/coValueCore/coValueCore.ts`

## Testing

- [ ] **Task 10**: Write unit tests for `UnsyncedCoValuesTracker` (US1, US3, US4)
  - Test `add(id, peerId)`, `remove(id, peerId)`, `getAll()`, `isAllSynced()` operations
  - Test persistence using `StorageAPI.trackCoValueSyncState`
  - Test subscription notifications (both per-CoValue and global)
  - Test that listeners are called immediately with current state on subscription
  - Test error handling when storage is unavailable (fallback to in-memory only)
  - Create `packages/cojson/src/sync/__tests__/UnsyncedCoValuesTracker.test.ts`

- [ ] **Task 11**: Write integration tests for sync tracking (US1, US2)
  - Test that CoValues are tracked when `syncContent()` is called
  - Test that CoValues are removed when they become synced to all peers
  - Test `resumeUnsyncedCoValues()` loads and resumes syncing
  - Test integration with `SyncStateManager`
  - Create/update integration tests in `packages/cojson/src/tests/`

- [ ] **Task 12**: Write tests for subscription APIs (US3, US4)
  - Test `CoValueCore.subscribeToSyncState()` notifies on status changes
  - Test `SyncManager.subscribeToSyncState()` notifies when all synced
  - Test immediate callback with current state on subscription
  - Test unsubscribe functionality

- [ ] **Task 13**: Write tests for refactored `waitForSync` (US5)
  - Test that `waitForSync()` resolves when CoValue becomes synced
  - Test timeout handling if applicable
  - Test backward compatibility with existing usage

- [ ] **Task 14**: Write E2E test for offline/online scenario (US1, US2)
  - Test scenario: user goes offline, makes changes, closes app, reopens app
  - Verify unsynced CoValues are tracked and resumed on restart
  - Test with partial CoValue loading
  - Update or create E2E test in `tests/e2e/`

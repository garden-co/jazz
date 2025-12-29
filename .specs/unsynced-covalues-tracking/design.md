# Design: Track Unsynced CoValues & Resume Sync

## Overview

This design implements automatic tracking of CoValues with unsynced changes and provides mechanisms to resume syncing them on app restart. The solution integrates with the existing sync infrastructure and provides reactive APIs for monitoring sync state.

The core idea is to maintain a set of CoValue IDs that have pending changes not yet fully synced to all persistent server peers. This set is persisted across app restarts and used to:
1. Automatically resume syncing on startup
2. Provide efficient sync state queries
3. Enable reactive sync state subscriptions

## Architecture / Components

### 1. Extended StorageAPI Interface

Extend the existing `StorageAPI` interface to include methods for tracking unsynced CoValues.

**Location:** `packages/cojson/src/storage/types.ts`

**New Methods:**
```typescript
export interface StorageAPI {
  // ... existing methods ...
  
  trackCoValuesSyncState(operations: Array<{ id: RawCoID; peerId: PeerID; synced: boolean }>): void;
  getUnsyncedCoValueIDs(callback: (data: RawCoID[]) => void);
  stopTrackingSyncState(id: RawCoID): void;
}
```

**Implementation Strategy:**
- Storage implementations will add these methods to persist the set of unsynced CoValue IDs
- For IndexedDB: Add a new object store `"unsyncedCoValues"`
- For SQLite: Add a table `unsynced_covalues`

### 2. UnsyncedCoValuesTracker

A new class that manages the set of unsynced CoValue IDs.

**Location:** `packages/cojson/src/sync/UnsyncedCoValuesTracker.ts`

**Responsibilities:**
- Maintain an in-memory Set of unsynced CoValue IDs (including peers pending sync)
- Persist the set to storage using `StorageAPI.trackCoValuesSyncState` if available. Persistence is batched and performed asynchronously, to avoid the cost of N extra storage writes per update (where N is the number of peers). 
- Load persisted unsynced CoValues on initialization using `StorageAPI.getUnsyncedCoValueIDs` + `LocalNode.loadCoValueCore` if storage is available
- Notify listeners when the set changes

**Key Methods:**
- `constructor(getStorage: () => StorageAPI | undefined)`: Initialize with storage getter for persistence
- `add(id: RawCoID, peerId: PeerID)`: Add a CoValue to the unsynced set (queues for batched persistence)
- `remove(id: RawCoID, peerId: PeerID)`: Remove a CoValue from the unsynced set (queues for batched persistence)
- `getAll()`: Returns all unsynced CoValue IDs 
- `isAllSynced()`: Check if all CoValues are synced (O(1), returns `size() === 0`)
- `private flush()`: Flush all pending persistence operations in a batch
- `subscribe(id: RawCoID, listener: (synced: boolean) => void)`: Subscribe to changes in whether a CoValue is synced
- `subscribe(listener: (synced: boolean) => void)`: Subscribe to changes in whether all CoValues are synced

### 3. Integration with SyncManager

**Location:** `packages/cojson/src/sync.ts`

**Changes:**
- Add `unsyncedTracker: UnsyncedCoValuesTracker` property to `SyncManager`
- Initialize tracker in `SyncManager` constructor: `new UnsyncedCoValuesTracker(local.storage, this)`
- Update `syncContent()` method to keep track of unsynced CoValues created locally:
 ```ts
 syncContent(content: NewContentMessage) {
  const coValue = this.local.getCoValue(content.id);

  this.storeContent(content);

  this.trackSyncState(coValue.id);

  // ...
 }

 trackSyncState(coValueId: RawCoID): void {
  for (const peer of this.getPersistentServerPeers()) {
    this.unsyncedTracker.add(coValueId, peer.id);

    const unsubscribe = this.syncState.subscribeToPeerUpdates(
      coValueId,
      peer.id,
      (_knownState, syncState) => {
        if (syncState.uploaded) {
          this.unsyncedTracker.remove(coValueId, peer.id);
          unsubscribe();
        }
      },
    );
  }
 }
 ```
- Update `handleNewContent` method to keep track of unsynced CoValues received from other peers:
 ```ts
 handleNewContent(
    msg: NewContentMessage,
    from: PeerState | "storage" | "import",
  ) {
    const coValue = this.local.getCoValue(msg.id);
    const peer = from === "storage" || from === "import" ? undefined : from;
    const sourceRole =
      from === "storage"
        ? "storage"
        : from === "import"
          ? "import"
          : peer?.role;
    
    // ...

    if (from !== "storage" && hasNewContent) {
      this.storeContent(validNewContent);
    }

    if (sourceRole === "client" && hasNewContent) {
      this.trackSyncState(coValue.id);
    }

    // ...
  }
 ```
- Add method `async resumeUnsyncedCoValues()` to load and resume syncing unsynced CoValues. This happens asynchronously and doesn't block initialization
- Call `resumeUnsyncedCoValues` as part of `SyncManager.startPeerReconciliation`.

**`resumeUnsyncedCoValues()` implementation:**

```typescript
async resumeUnsyncedCoValues(): Promise<void> {
  if (!this.local.storage) {
    // No storage available, skip resumption
    return;
  }

  await new Promise<void>((resolve, reject) => {
    // Load all persisted unsynced CoValues from storage
    this.local.storage!.getUnsyncedCoValueIDs((unsyncedCoValueIDs) => {
      if (unsyncedCoValueIDs.length === 0) {
        resolve();
        return;
      }

      const BATCH_SIZE = 10;
      let processed = 0;

      const processBatch = async () => {
        const batch = unsyncedCoValueIDs.slice(
          processed,
          processed + BATCH_SIZE,
        );
        
        await Promise.all(
          batch.map(async (coValueId) => {
            try {
              // Load the CoValue from storage (this will trigger sync if peers are connected)
              const coValue = await this.local.loadCoValueCore(coValueId);

              if (coValue.isAvailable()) {
                // CoValue was successfully loaded. Resume tracking sync state for this CoValue
                // This will add it back to the tracker and set up subscriptions
                this.trackSyncState(coValueId);
              } else {
                // CoValue not found in storage. Remove all peer entries for this CoValue
                this.local.storage!.stopTrackingSyncState(coValueId);
              }
            } catch (error) {
              // Handle errors gracefully - log but don't fail the entire resumption
              logger.warn(
                `Failed to resume sync for CoValue ${coValueId}:`,
                error,
              );
              this.local.storage!.stopTrackingSyncState(coValueId);
            }
          }),
        );

        processed += batch.length;

        if (processed < unsyncedCoValueIDs.length) {
          // Process next batch asynchronously to avoid blocking
          setTimeout(processBatch, 0);
        } else {
          resolve();
        }
      };

      processBatch().catch(reject);
    });
  });
}
```

### 4. Sync Status Subscriptions

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts` and `packages/cojson/src/sync.ts`

**CoValueCore.subscribeToSyncState:**
- Subscribe to changes in whether this specific CoValue is synced
- Uses `syncManager.unsyncedTracker.subscribe(coValueId)` to get notified when the CoValue's sync state changes
- Calls listener immediately with current state on subscription

**SyncManager.subscribeToSyncState:**
- Subscribe to changes in whether all CoValues are synced
- Uses `syncManager.unsyncedTracker.subscribe()` to get notified when the unsynced set changes
- Calls listener immediately with current state on subscription (check `unsyncedTracker.isAllSynced()`)

### 5. Refactored waitForSync

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts` and `packages/cojson/src/sync.ts`

**Changes:**
- Replace the current implementation of `CoValueCore.waitForSync()` based on CoValue subscription with a simple call to `syncManager.unsyncedTracker.subscribe(coValueId)`.

## Data Model

### UnsyncedCoValuesTracker In-Memory Representation

The tracker maintains an in-memory data structure that maps each unsynced CoValue to the set of peers it's unsynced to:

```typescript
class UnsyncedCoValuesTracker {
  // Map from CoValue ID to Set of Peer IDs that the CoValue is unsynced to
  private unsynced: Map<RawCoID, Set<PeerID>> = new Map();
  private coValueListeners: Map<RawCoID, (synced: boolean) => void> = new Map();
  private globalListeners: (synced: boolean) => void = new Set();
  private storage?: StorageAPI;
}
```

### Storage Layout

Storage persists unsynced CoValue-to-peer relationships as individual rows, with one row per (CoValue ID, Peer ID) pair.

**IndexedDB Schema:**
- Object store: `"unsyncedCoValues"`
- Key: `[coValueId, peerId]` (composite key)
- Value: `{ coValueId: RawCoID, peerId: PeerID }`
- Indexes:
  - Index on `coValueId` for efficient queries by CoValue
  - Index on `peerId` for efficient queries by peer (optional, for cleanup)

**SQLite Schema:**
```sql
CREATE TABLE unsynced_covalues (
  co_value_id TEXT NOT NULL,
  peer_id TEXT NOT NULL,
  PRIMARY KEY (co_value_id, peer_id)
);

CREATE INDEX idx_unsynced_covalues_co_value_id ON unsynced_covalues(co_value_id);
```

**Storage Operations:**
- `trackCoValuesSyncState(operations)`:
  - Takes an array of operations `{ id: RawCoID, peerId: PeerID, synced: boolean }[]`
  - Executes all operations in a single transaction
  - For each operation:
    - If `synced === true`: DELETE row where `co_value_id = id AND peer_id = peerId`
    - If `synced === false`: INSERT OR REPLACE row with `(id, peerId)`
- `getUnsyncedCoValueIDs(callback)`:
  - Query all distinct `co_value_id` values (SELECT DISTINCT co_value_id)
  - Return array of unique CoValue IDs that have at least one unsynced peer

**Example Storage Data:**
```
co_value_id    | peer_id
---------------|----------
co_abc123      | peer1
co_abc123      | peer2
co_def456      | peer1
```

This represents:
- `co_abc123` is unsynced to `peer1` and `peer2`
- `co_def456` is unsynced to `peer1`

**Loading on Startup:**
1. Call `getUnsyncedCoValueIDs()` to get all CoValue IDs with unsynced peers
2. For each CoValue ID, query all peer IDs: `SELECT peer_id WHERE co_value_id = ?`
3. Reconstruct the in-memory Map structure
4. Load each CoValue and resume syncing

## Error Handling / Testing Strategy

### Error Handling

1. **Storage Errors:**
   - If persistence fails, log error but continue with in-memory tracking
   - On load failure, start with empty set (graceful degradation)
   - Don't block LocalNode initialization if persistence fails

2. **Missing CoValues:**
   - Handle gracefully when trying to load non-existent CoValues

3. **Peer Connection Issues:**
   - Continue tracking even when peers are disconnected
   - Resume syncing when peers reconnect

4. **Race Conditions:**
   - Use atomic operations for add/remove from Set
   - Ensure persistence operations don't interfere with tracking updates

### Testing Strategy

1. **Unit Tests:**
   - Test `UnsyncedCoValuesTracker` operations
   - Test persistence and loading
   - Test subscription notifications
   - Test sync state determination logic

2. **Integration Tests:**
   - Test integration with `SyncManager` and `SyncStateManager`
   - Test that CoValues are tracked when they become unsynced
   - Test that CoValues are removed when they become synced
   - Test resumption on LocalNode initialization
   - Test subscription APIs (`subscribeToSyncState`)
   - Test refactored `waitForSync`

3. **E2E Tests:**
   - Test offline/online scenario with partial CoValue loading
   - Test that unsynced CoValues are synced after app restart

4. **Performance Tests:**
   - Test with large numbers of unsynced CoValues
   - Test persistence/loading performance
   - Test subscription performance with many listeners
   - Test polling performance in `waitForSync`

5. **Platform Tests:**
   - Test on web (IndexedDB storage)
   - Test on Node.js (SQLite storage)

### Edge Cases

1. **Very large unsynced set** - Should handle efficiently, without a noticeable impact on app startup
2. **Rapid sync state changes** - Should debounce/throttle persistence
3. **Multiple sessions sharing storage** - IndexedDB is shared across tabs, but this shouldn't be a problem as we don't need to keep the in-memory unsynced CoValue list in sync with storage (we only use storage for persistence
across app restarts)
4. **Storage unavailable** - Should fall back to in-memory only

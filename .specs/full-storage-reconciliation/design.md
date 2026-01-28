# Full Storage Reconciliation

## Overview

Full Storage Reconciliation ensures all local CoValue content (both in memory and in storage) is uploaded to the server peers. It's implemented by adding a new `SyncManager.ensureCoValuesSync()` function. This is separate from peer reconciliation and focuses on uploading local changes to servers, without subscribing to peer changes.

The key mechanism is a new `skipSubscription` flag on load messages that tells the server to only return its known state (not send new content), allowing the client to determine if it needs to upload its content.

## Implementation Steps

### 1. Enumerating all CoValues

#### 1.1. Add Storage API Method

**File:** `packages/cojson/src/storage/types.ts`

Add a new method to `StorageAPI` interface:

```typescript
/**
 * Get all CoValue IDs currently stored in storage.
 * Used for full storage reconciliation.
 */
getAllCoValueIDs(callback: (ids: { id: RawCoID }[]) => void): void;
```

Also add to `DBClientInterfaceSync` and `DBClientInterfaceAsync`:

```typescript
getAllCoValueIDs(): { id: RawCoID}[];  // sync
getAllCoValueIDs(): Promise<{ id: RawCoID }[]>;  // async
```

#### 1.2. Implement getAllCoValueIDs in Storage Backends

##### SQLite Sync Client

**File:** `packages/cojson/src/storage/sqlite/client.ts`

```typescript
getAllCoValueIDs(): { id: RawCoID }[] {
  return this.db.query<{ id: RawCoID }>(
    "SELECT id FROM coValues",
    [],
  );
}
```

##### SQLite Async Client

**File:** `packages/cojson/src/storage/sqliteAsync/client.ts`

```typescript
async getAllCoValueIDs(): Promise<{ id: RawCoID }[]> {
  return this.db.query<{ id: RawCoID }>(
    "SELECT id FROM coValues",
    [],
  );
}
```

##### Storage Wrappers

**Files:**
- `packages/cojson/src/storage/storageSync.ts`
- `packages/cojson/src/storage/storageAsync.ts`

```typescript
getAllCoValueIDs(callback: (ids: { id: RawCoID }[]) => void): void {
  const ids = this.dbClient.getAllCoValueIDs();
  callback(ids);
}
```

For async:
```typescript
getAllCoValueIDs(callback: (ids: { id: RawCoID }[]) => void): void {
  this.dbClient.getAllCoValueIDs().then(callback);
}
```

##### IndexedDB Client

**File:** `packages/cojson-storage-indexeddb/src/idbClient.ts`

```typescript
async getAllCoValueIDs(): Promise<{ id: RawCoID }[]> {
  return queryIndexedDbStore<StoredCoValueRow[]>(
    this.db,
    "coValues",
    (store) => store.getAll(),
  );
}
```

### 2. Skipping Subscriptions

#### 2.1. Add skipSubscription Flag to LoadMessage

**File:** `packages/cojson/src/sync.ts`

Modify the `LoadMessage` type:

```typescript
export type LoadMessage = {
  action: "load";
  skipSubscription?: boolean;  // If true, server should only return known state, not send new content
} & CoValueKnownState;
```

#### 2.2. Track Non-Subscribed CoValues in PeerState

**File:** `packages/cojson/src/PeerState.ts`

Add a Set to track CoValues that were checked with `skipSubscription`:

```typescript
export class PeerState {
  // ... existing fields ...
  
  /**
   * CoValues that were checked with skipSubscription flag.
   * These should not be considered subscribed even if they have a known state.
   */
  private readonly nonSubscribedCoValues = new Set<RawCoID>();
  
  /**
   * Mark a CoValue as non-subscribed (checked with skipSubscription).
   */
  markNonSubscribed(id: RawCoID): void {
    this.nonSubscribedCoValues.add(id);
  }
  
  /**
   * Mark a CoValue as subscribed (remove from non-subscribed set).
   * Used when a normal load message is received after a skipSubscription load.
   */
  markSubscribed(id: RawCoID): void {
    this.nonSubscribedCoValues.delete(id);
  }
  
  /**
   * Check if a CoValue is subscribed to this peer.
   * Returns false if the CoValue was checked with skipSubscription.
   */
  isCoValueSubscribedToPeer(id: RawCoID): boolean {
    if (this.nonSubscribedCoValues.has(id)) {
      return false;
    }
    return this._knownStates.has(id);
  }
}
```

#### 2.3. Modify handleLoad to respect skipSubscription

**File:** `packages/cojson/src/sync.ts`

Modify `handleLoad` method to check for `skipSubscription` flag:

```typescript
handleLoad(msg: LoadMessage, peer: PeerState) {
  const coValue = this.local.getCoValue(msg.id);
  
  // Check if already subscribed BEFORE setting known state
  const wasAlreadySubscribed = peer.isCoValueSubscribedToPeer(msg.id);
  
  peer.setKnownState(msg.id, knownStateFrom(msg));

  // If skipSubscription is true, only return known state without subscribing
  if (msg.skipSubscription) {
    // Only mark as non-subscribed if it wasn't already subscribed
    // This preserves existing subscriptions
    if (!wasAlreadySubscribed) {
      peer.markNonSubscribed(msg.id);
    }
    
    // Get known state from memory or load from storage
    if (coValue.isAvailable()) {
      const knownState = coValue.knownState();
      peer.trackToldKnownState(msg.id);
      this.trySendToPeer(peer, {
        action: "known",
        ...knownState,
      });
    } else {
      // Load known state from storage asynchronously
      coValue.getKnownStateFromStorage((storageKnownState) => {
        const knownState = storageKnownState ?? emptyKnownState(msg.id);
        peer.trackToldKnownState(msg.id);
        this.trySendToPeer(peer, {
          action: "known",
          ...knownState,
        });
      });
    }
    return;
  }

  // If this is a normal load (not skipSubscription), ensure it's marked as subscribed
  // This handles the case where a normal load comes after a skipSubscription load
  peer.markSubscribed(msg.id);

  // Existing logic for normal load messages...
  // (rest of the method remains the same)
}
```

**Key behavior when `skipSubscription` is true:**
- Store the client's known state (needed for tracking)
- Only mark as non-subscribed if not already subscribed (preserves existing subscriptions)
- Do NOT load CoValue from storage or peers
- Do NOT send new content to the requesting peer
- Only return the server's known state for that CoValue
- This allows the client to compare known states and determine if it needs to upload
- If the CoValue is already subscribed, the subscription remains active

#### 2.4. Modify sendLoadRequest to Support skipSubscription

**File:** `packages/cojson/src/PeerState.ts`

Modify the existing `sendLoadRequest` method to accept an optional `skipSubscription` parameter:

```typescript
sendLoadRequest(
  coValue: CoValueCore, 
  mode?: LoadMode,
  skipSubscription?: boolean
): void {
  this.toldKnownState.add(coValue.id);
  this.loadRequestSent.add(coValue.id);
  this.loadQueue.enqueue(
    coValue,
    () => {
      this.pushOutgoingMessage({
        action: "load",
        ...(skipSubscription ? { skipSubscription: true } : {}),
        ...coValue.knownStateWithStreaming(),
      });
    },
    mode,
  );
}
```

### 3. Implement ensureCoValuesSync

**File:** `packages/cojson/src/sync.ts`

Add new method to `SyncManager`:

```typescript
/**
 * Ensures all CoValues (both in memory and in storage) are synced to server peers.
 * This checks known states and uploads local content to servers without
 * subscribing to server changes.
 */
ensureCoValuesSync(): void {
  const processedIds = new Set<RawCoID>();

  // Process CoValues in memory first
  for (const coValue of this.local.allCoValues()) {
    if (coValue.isAvailable()) {
      this.processCoValueForSync(coValue);
      processedIds.add(coValue.id);
    }
  }

  // Then process CoValues from storage
  this.local.storage?.getAllCoValueIDs((allIds) => {
    for (const { id } of allIds) {
      // Skip if already processed from memory
      if (processedIds.has(id)) {
        continue;
      }

      const coValue = this.local.getCoValue(id);
      
      // Load known state from storage without loading full content
      coValue.getKnownStateFromStorage((storageKnownState) => {
        if (!storageKnownState) {
          return;
        }
        
        this.processCoValueForSync(id, storageKnownState);
      });
    }
  });
}

/**
 * Helper to process a single CoValue for sync with server peers.
 */
private processCoValueForSync(coValue: CoValueCore): void {
  const serverPeers = this.getPersistentServerPeers(coValue.id);
  for (const peer of serverPeers) {
    const isSynced = this.syncState.isSynced(peer, coValue.id);
    
    if (!isSynced) {
      // Send load request with skipSubscription to get server's known state,
      // once it's received, we'll send the missing content if necessary
      peer.sendLoadRequest(coValue, "low-priority", true);
    }
  }
}
```

### 4. Handle Known State Response from skipSubscription Load

**File:** `packages/cojson/src/sync.ts`

The existing `handleKnownState` method works for storage reconciliation. When we receive a known state response from an `skipSubscription` load, we send the content the server's missing.

## Design Considerations

1. **No Subscription**: When `skipSubscription` is true, the server should not add the client to its subscription list for that CoValue. This prevents the server from pushing future updates.

2. **Known State Comparison**: The client compares its known state with the server's known state to determine if upload is needed. For in-memory CoValues this comparison happens before sending the load request, for CoValues in storage it happens
after receiving the known state from the server.

3. **Loading Strategy**: For CoValues not in memory, we use `getKnownStateFromStorage` to avoid full loads. Only load full content when we need to send it to the server.

4. **Performance**: For large databases, consider batching the processing of CoValues to avoid blocking.

## Testing

- Test `ensureCoValuesSync` with CoValues only in storage
- Test with CoValues already in memory
- Test with mixed scenarios (some synced, some not)
- Verify that `skipSubscription` load requests don't cause server to send content
- Verify that client correctly uploads content when needed

# Design: CoValue Lazy Loading Optimization

## Overview

This design implements lazy loading of CoValues from storage to optimize memory usage and response times. Instead of fully loading CoValues (header + all transactions) when handling `load` requests from peers, the system first loads only the `knownState` (header presence + session transaction counts) to determine if the peer actually needs new content.

The core optimization flow:
1. Peer sends `load` request with their `knownState`
2. Server loads only the storage's `knownState` for that CoValue
3. Compare: Does the peer already have everything?
   - **Yes** → Reply with `known` message, skip full load
   - **No** → Load full CoValue from storage, send new content

This is particularly beneficial for sync servers that handle many CoValues, where clients often already have the latest data.

## Architecture / Components

### 1. New Storage API Method: `loadKnownState`

**Location:** `packages/cojson/src/storage/types.ts`

Add a new method to the `StorageAPI` interface:

```typescript
export interface StorageAPI {
  // ... existing methods ...

  /**
   * Load only the knownState (header presence + session counters) for a CoValue.
   * This is more efficient than load() when we only need to check if a peer needs new content.
   * 
   * @param id - The CoValue ID
   * @param callback - Called with the knownState, or undefined if CoValue not found
   */
  loadKnownState(
    id: string,
    callback: (knownState: CoValueKnownState | undefined) => void,
  ): void;
}
```

### 2. New DB Client Method: `getCoValueKnownState`

**Location:** `packages/cojson/src/storage/types.ts`

Add to both `DBClientInterfaceSync` and `DBClientInterfaceAsync`:

```typescript
export interface DBClientInterfaceSync {
  // ... existing methods ...

  /**
   * Get the knownState for a CoValue without loading transactions.
   * Returns undefined if the CoValue doesn't exist.
   */
  getCoValueKnownState(coValueId: string): CoValueKnownState | undefined;
}

export interface DBClientInterfaceAsync {
  // ... existing methods ...

  getCoValueKnownState(coValueId: string): Promise<CoValueKnownState | undefined>;
}
```

### 3. SQLite Client Implementation

**Location:** `packages/cojson/src/storage/sqlite/client.ts`

```typescript
getCoValueKnownState(coValueId: RawCoID): CoValueKnownState | undefined {
  // First check if the CoValue exists
  const coValueRow = this.db.get<{ rowID: number }>(
    "SELECT rowID FROM coValues WHERE id = ?",
    [coValueId],
  );

  if (!coValueRow) {
    return undefined;
  }

  // Get all session counters without loading transactions
  const sessions = this.db.query<{ sessionID: SessionID; lastIdx: number }>(
    "SELECT sessionID, lastIdx FROM sessions WHERE coValue = ?",
    [coValueRow.rowID],
  );

  const knownState: CoValueKnownState = {
    id: coValueId,
    header: true,
    sessions: {},
  };

  for (const session of sessions) {
    knownState.sessions[session.sessionID] = session.lastIdx;
  }

  return knownState;
}
```

**Location:** `packages/cojson/src/storage/sqliteAsync/client.ts`

Same implementation but with `async/await`.

### 4. StorageApiSync Implementation

**Location:** `packages/cojson/src/storage/storageSync.ts`

The sync implementation doesn't need deduplication since calls complete synchronously before another can start.

```typescript
loadKnownState(
  id: string,
  callback: (knownState: CoValueKnownState | undefined) => void,
): void {
  // Check in-memory cache first
  const cached = this.knownStates.getCachedKnownState(id);
  if (cached) {
    callback(cached);
    return;
  }

  // Load from database (synchronous - no deduplication needed)
  const knownState = this.dbClient.getCoValueKnownState(id);
  
  if (knownState) {
    // Cache for future use
    this.knownStates.setKnownState(id, knownState);
  }

  callback(knownState);
}
```

### 5. StorageApiAsync Implementation

**Location:** `packages/cojson/src/storage/storageAsync.ts`

The async implementation includes deduplication to prevent multiple parallel database queries for the same CoValue ID.

```typescript
// Track pending loads to deduplicate concurrent requests
private pendingKnownStateLoads = new Map<string, Promise<CoValueKnownState | undefined>>();

loadKnownState(
  id: string,
  callback: (knownState: CoValueKnownState | undefined) => void,
): void {
  // Check in-memory cache first
  const cached = this.knownStates.getCachedKnownState(id);
  if (cached) {
    callback(cached);
    return;
  }

  // Check if there's already a pending load for this ID (deduplication)
  const pending = this.pendingKnownStateLoads.get(id);
  if (pending) {
    pending.then(callback);
    return;
  }

  // Start new load and track it for deduplication
  const loadPromise = this.dbClient.getCoValueKnownState(id).then((knownState) => {
    if (knownState) {
      // Cache for future use
      this.knownStates.setKnownState(id, knownState);
    }
    // Remove from pending map after completion
    this.pendingKnownStateLoads.delete(id);
    return knownState;
  });

  this.pendingKnownStateLoads.set(id, loadPromise);
  loadPromise.then(callback);
}
```

### 6. New `lazyLoad` Method in CoValueCore

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

Add a new method that loads only the knownState from storage, enabling callers to decide whether a full load is needed:

```typescript
/**
 * Lazily load only the knownState from storage without loading full transaction data.
 * This is useful for checking if a peer needs new content before committing to a full load.
 * 
 * Caching and deduplication are handled at the storage layer (see StorageApiSync/StorageApiAsync).
 * 
 * @param done - Callback with the storage knownState, or undefined if not found in storage
 */
getKnownStateFromStorage(done: (knownState: CoValueKnownState | undefined) => void) {
  if (!this.node.storage) {
    done(undefined);
    return;
  }

  // If already available in memory, return the current knownState
  if (this.isAvailable()) {
    done(this.knownState());
    return;
  }

  // Delegate to storage - caching is handled at storage level
  this.node.storage.loadKnownState(this.id, done);
}
```

**Helper function** (in `knownState.ts`, reuses existing `isKnownStateSubsetOf`):

```typescript
/**
 * Check if the peer already has all the content from storage.
 * Returns true if the peer has at least as many transactions as storage for all sessions.
 */
export function peerHasAllContent(
  storageKnownState: CoValueKnownState,
  peerKnownState: CoValueKnownState | undefined,
): boolean {
  if (!peerKnownState) {
    return false;
  }

  // Check if peer has the header
  if (!peerKnownState.header && storageKnownState.header) {
    return false;
  }

  // Check all sessions - peer must have at least as many transactions as storage
  return isKnownStateSubsetOf(
    storageKnownState.sessions,
    peerKnownState.sessions,
  );
}
```

### 7. Modified `handleLoad` in SyncManager

**Location:** `packages/cojson/src/sync.ts`

The `handleLoad` method uses `getKnownStateFromStorage` to check storage before doing a full load. The lazy load logic lives in `SyncManager` since it's sync-related:

```typescript
handleLoad(msg: LoadMessage, peer: PeerState) {
  peer.setKnownState(msg.id, knownStateFrom(msg));
  const coValue = this.local.getCoValue(msg.id);

  // Fast path: CoValue is already in memory
  if (coValue.isAvailable()) {
    this.sendNewContent(msg.id, peer);
    return;
  }

  const peerKnownState = peer.getOptimisticKnownState(msg.id);

  // Check storage knownState before doing full load
  coValue.getKnownStateFromStorage((storageKnownState) => {
    if (!storageKnownState) {
      // Not in storage, try loading from peers
      this.loadFromPeersAndRespond(msg.id, peer, coValue);
      return;
    }

    // Check if peer already has all content
    if (peerHasAllContent(storageKnownState, peerKnownState)) {
      // Peer already has everything - reply with known message, no full load needed
      peer.trackToldKnownState(msg.id);
      this.trySendToPeer(peer, {
        action: "known",
        ...storageKnownState,
      });
      return;
    }

    // Peer needs content - do full load from storage
    coValue.loadFromStorage((found) => {
      if (found && coValue.isAvailable()) {
        this.sendNewContent(msg.id, peer);
      } else {
        this.loadFromPeersAndRespond(msg.id, peer, coValue);
      }
    });
  });
}

/**
 * Helper to load from peers and respond appropriately.
 */
private loadFromPeersAndRespond(
  id: RawCoID,
  peer: PeerState,
  coValue: CoValueCore,
) {
  const peers = this.getServerPeers(id, peer.id);
  coValue.loadFromPeers(peers);

  const handleLoadResult = () => {
    if (coValue.isAvailable()) {
      this.sendNewContent(id, peer);
      return;
    }
    this.handleLoadNotFound(id, peer);
  };

  if (peers.length > 0) {
    coValue.waitForAvailableOrUnavailable().then(handleLoadResult);
  } else {
    handleLoadResult();
  }
}

/**
 * Handle case when CoValue is not found.
 */
private handleLoadNotFound(id: RawCoID, peer: PeerState) {
  peer.trackToldKnownState(id);
  this.trySendToPeer(peer, {
    action: "known",
    id,
    header: false,
    sessions: {},
  });
}
```

### 8. Modified `handleNewContent` in SyncManager

**Location:** `packages/cojson/src/sync.ts`

The current implementation already partially handles loading from storage for garbage-collected values. We update it to use `getKnownStateFromStorage` for efficiency:

```typescript
handleNewContent(
  msg: NewContentMessage,
  from: PeerState | "storage" | "import",
) {
  const coValue = this.local.getCoValue(msg.id);
  const peer = from === "storage" || from === "import" ? undefined : from;
  
  // ... existing code ...

  if (!coValue.hasVerifiedContent()) {
    if (!msg.header) {
      // Only check storage if content came from a peer or import (not storage itself - would be circular)
      if (from !== "storage") {
        // Use getKnownStateFromStorage to check if CoValue exists in storage
        // This is more efficient than getKnownState as it queries the DB if not cached
        coValue.getKnownStateFromStorage((storageKnownState) => {
          if (storageKnownState) {
            // CoValue exists in storage but was garbage collected from memory
            // Do full load before processing the new content
            coValue.loadFromStorage((found) => {
              if (found) {
                this.handleNewContent(msg, from);
              } else {
                logger.error("Known CoValue not found in storage", { id: msg.id });
              }
            });
          } else {
            // CoValue not in storage, ask peer for full content
            this.requestFullContent(msg.id, peer);
          }
        });
        return;
      }
      // Content from storage without header - shouldn't happen normally
      return;
    }

    // ... rest of existing code for handling new CoValue with header ...
  }

  // ... rest of existing code ...
}

private requestFullContent(id: RawCoID, peer: PeerState | undefined) {
  if (peer) {
    this.trySendToPeer(peer, {
      action: "known",
      isCorrection: true,
      id,
      header: false,
      sessions: {},
    });
  } else {
    logger.error(
      "Received new content with no header on a missing CoValue",
      { id },
    );
  }
}
```

## Data Model

### CoValueKnownState Structure

The existing `CoValueKnownState` type is used throughout:

```typescript
type CoValueKnownState = {
  id: RawCoID;
  header: boolean;
  sessions: { [sessionID: SessionID]: number };
};
```

- `id`: The CoValue's unique identifier
- `header`: `true` if the CoValue header exists
- `sessions`: Map of session IDs to transaction counts

### Database Queries

**Get KnownState (Sync - SQLite):**
```sql
-- Check CoValue exists
SELECT rowID FROM coValues WHERE id = ?;

-- Get session counts
SELECT sessionID, lastIdx FROM sessions WHERE coValue = ?;
```

**Comparison with Full Load:**
| Operation | Full Load | KnownState Load |
|-----------|-----------|-----------------|
| Tables queried | coValues, sessions, transactions, signatureAfter | coValues, sessions |
| Data returned | Header + all transactions | Header flag + session counts |
| Memory used | Proportional to transaction count | Constant (small) |

## Error Handling / Testing Strategy

### Error Handling

1. **Storage Unavailable:**
   - If `storage` is undefined, fall back to existing behavior (load from peers)
   - Don't block or fail the load request

2. **KnownState Load Fails:**
   - Treat as "not found" and fall back to full load or peer loading
   - Log warning but continue operation

3. **Race Conditions:**
   - Handle case where CoValue becomes available in memory while waiting for storage callback
   - Re-check `coValue.isAvailable()` after async operations

4. **Stale KnownState Cache:**
   - The in-memory `StorageKnownState` cache is updated after full loads
   - If cache is stale, the comparison might be incorrect, but this only results in unnecessary full loads (no data loss)

### Testing Strategy

**Unit Tests:**
1. Test `getCoValueKnownState` returns correct structure
2. Test `getCoValueKnownState` returns undefined for non-existent CoValues
3. Test `loadKnownState` uses cache when available
4. Test `loadKnownState` queries DB when not cached
5. Test `peerHasAllContent` comparison logic with various scenarios:
   - Peer has everything → returns true
   - Peer missing header → returns false
   - Peer missing sessions → returns false
   - Peer has more than storage → returns true
6. Test `CoValueCore.getKnownStateFromStorage`:
   - Returns knownState when CoValue exists in storage
   - Returns undefined when CoValue doesn't exist
   - Returns current knownState when CoValue is already in memory
   - Waits for pending load if already in progress
7. Test `CoValueCore.lazyLoad`:
   - Calls `onUpToDate` when peer has all content
   - Calls `onNeedsContent` after full load when peer needs content
   - Calls `onNotFound` when CoValue not in storage

**Integration Tests:**
1. Test `handleLoad` skips full load when peer has all content
2. Test `handleLoad` does full load when peer needs content
3. Test `handleLoad` falls back to peers when not in storage
4. Test `handleNewContent` loads from storage for garbage-collected CoValues
5. Test `handleNewContent` requests full content when not in storage
6. Test full flow: client with up-to-date state → no DB transaction queries
7. Test full flow: client with stale state → full load occurs

**Performance Tests:**
1. Benchmark `loadKnownState` vs `load` for CoValues with many transactions
2. Measure memory usage reduction in sync server scenarios
3. Test with high load request volume

### Edge Cases

1. **Empty CoValue (header only, no transactions):**
   - `knownState.sessions` will be empty `{}`
   - Should still return valid knownState with `header: true`

2. **CoValue with streaming content:**
   - KnownState reflects committed transactions only
   - Streaming state is handled separately

3. **Concurrent modifications:**
   - Storage writes happen async; knownState might be slightly behind
   - This is acceptable - worst case is an unnecessary full load

4. **Large session count:**
   - A CoValue could have many sessions
   - Query is still efficient (indexed by coValue)

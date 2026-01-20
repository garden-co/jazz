# Design: Peer Reconciliation Lazy Load Optimization (v2)

## Overview

This design introduces explicit CoValue states (`garbageCollected` and `onlyKnownState`) to distinguish between CoValues we actually care about versus ones we just heard about. This prevents unnecessary data transfer during peer reconciliation by only restoring subscriptions for CoValues that were actively used.

**Problem with current approach:**
```
Client reconnects → iterates ALL CoValues in memory → for unavailable ones, sends LOAD
→ "unavailable" includes truly unknown CoValues we never cared about
→ Server loads and sends ALL content for every ID we ever heard of
```

**Root cause:** The original design treated all "unavailable" CoValues the same, but they're actually different:

| Type | Meaning | Has lastKnownState? | Optimization |
|------|---------|---------------------|--------------|
| **Unknown** | Just heard the ID, never had/wanted the data | ❌ No | **SKIPPED** - no LOAD sent during reconciliation |
| **GarbageCollected** | Had data in memory, was actively used, then GC'd | ✅ Yes | Sends last known state → server sends diff only |
| **OnlyKnownState** | Checked storage, have knownState saved, but not full content | ✅ Yes | Sends last known state → server sends diff only |

**New behavior:**
```
Client reconnects → iterates all CoValues → checks loadingState:
  - "available" → send LOAD with in-memory knownState
  - "garbageCollected" → send LOAD with lastKnownState (prevents full retransfer!)
  - "onlyKnownState" → send LOAD with lastKnownState (prevents full retransfer!)
  - "unknown" → SKIP (don't send LOAD - we never cared about this CoValue)
  - "unavailable"/"loading"/"errored" → send LOAD with empty knownState
```

**Key optimization:** GC'd and onlyKnownState CoValues now send their `lastKnownState`, enabling the server to send only the diff instead of all content.

**Design simplification:** The `knownState()` method is updated to return the `lastKnownState` when available, so callers can simply use `knownState()` in all cases without needing separate handling for different states.

## Integration with Lazy Storage Load

This design integrates with the **lazy-storage-load** spec (`/.specs/lazy-storage-load/design.md`). The `onlyKnownState` state emerges naturally from the existing `getKnownStateFromStorage` API:

- When `getKnownStateFromStorage` is called and **finds data in storage**:
  - Cache the returned knownState in `lastKnownState`
  - Mark the CoValue as `onlyKnownState`

This creates two complementary paths to the same outcome:

| Trigger | State Set | When it happens |
|---------|-----------|-----------------|
| `getKnownStateFromStorage` returns data | `onlyKnownState` | Server handles LOAD, checks storage for knownState |
| `unmount()` during GC | `garbageCollected` | Client GC removes full content from memory |

Both states have `lastKnownState` available, enabling synchronous access during peer reconciliation.

## Architecture / Components

### 1. loadingStatuses Unchanged

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

The `loadingStatuses` map remains unchanged - it only tracks loading state from sources (storage, peers):

```typescript
private readonly loadingStatuses = new Map<
  PeerID | "storage",
  | {
      type:
        | "unknown"
        | "pending"
        | "available"
        | "unavailable";
    }
  | {
      type: "errored";
      error: unknown;
    }
>();
```

**Note:** Both `garbageCollected` and `onlyKnownState` are tracked separately via `#lastKnownStateSource` (see Section 2) because they represent CoValue lifecycle states, not loading states from sources.

### 2. New Fields for Last Known State

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

When a CoValue becomes `garbageCollected` or `onlyKnownState`, we cache its `knownState` so we can use it synchronously during reconciliation without needing async storage lookups.

```typescript
// Tracks why we have lastKnownState (separate from loadingStatuses)
// - "garbageCollected": was in memory, got GC'd
// - "onlyKnownState": checked storage, found knownState, but didn't load full content
#lastKnownStateSource?: "garbageCollected" | "onlyKnownState";

// Cache the knownState when transitioning to garbageCollected/onlyKnownState
#lastKnownState?: CoValueKnownState;
```

**Why separate from `loadingStatuses`:**
- `loadingStatuses` tracks loading state from various **sources** (storage, peers)
- `garbageCollected` and `onlyKnownState` are about the **CoValue's state** - whether we have partial knowledge
- Keeping them separate maintains cleaner semantics and avoids confusion
- Both states share the same behavior: we have `lastKnownState` but not full content

The `knownState()` method is updated to use this cache (see Section 7), so callers can simply call `knownState()` without special handling.

### 3. Modified `unmount()` Method

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

Instead of just removing the CoValue from the map entirely, we delete it and replace it with a fresh "shell" CoValueCore in the `garbageCollected` state. This ensures proper cleanup of all internal state.

**Current code (problematic):**
```typescript
unmount(): boolean {
  // ... checks ...
  this.counter.add(-1, { state: this.loadingState });
  this.node.internalDeleteCoValue(this.id);  // Removes from map entirely!
  return true;
}
```

**New approach:**
```typescript
unmount(): boolean {
  if (this.listeners.size > 0) {
    return false;
  }

  for (const dependant of this.dependant) {
    if (this.node.hasCoValue(dependant)) {
      return false;
    }
  }

  if (!this.node.syncManager.isSyncedToServerPeers(this.id)) {
    return false;
  }

  // Cache the knownState before deleting
  const lastKnownState = this.knownState();
  
  this.counter.add(-1, { state: this.loadingState });
  
  // Delete the old CoValueCore (this also calls storage?.onCoValueUnmounted)
  this.node.internalDeleteCoValue(this.id);
  
  // Create a new "shell" CoValueCore in garbageCollected state
  this.node.createGarbageCollectedCoValue(this.id, lastKnownState);

  return true;
}
```

### 4. New Method in LocalNode: `createGarbageCollectedCoValue`

**Location:** `packages/cojson/src/localNode.ts`

```typescript
/**
 * Create a CoValueCore in the garbageCollected state.
 * Used after unmounting to keep track of CoValues we care about.
 */
createGarbageCollectedCoValue(id: RawCoID, lastKnownState: CoValueKnownState) {
  const coValue = new CoValueCore(id, this);
  coValue.setGarbageCollectedState(lastKnownState);
  this.coValues.set(id, coValue);
  return coValue;
}
```

### 5. New Method in CoValueCore: `setGarbageCollectedState`

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

```typescript
/**
 * Initialize this CoValueCore as a garbageCollected shell.
 * Called when creating a replacement CoValueCore after unmounting.
 */
setGarbageCollectedState(lastKnownState: CoValueKnownState) {
  this.#lastKnownStateSource = "garbageCollected";
  this.#lastKnownState = lastKnownState;
  this.updateCounter(null);
}
```

**Why use `#lastKnownStateSource` instead of `loadingStatuses`:**
- `loadingStatuses` is for tracking loading state from sources (storage, peers)
- `garbageCollected` is a lifecycle state - the CoValue existed, was used, then GC'd
- This separation keeps the semantics clean and avoids confusion

**Why delete and create new CoValueCore:**
- CoValueCore has many internal fields (listeners, loadingStatuses for peers, dependencies, streaming state, etc.)
- Just nulling `_verified` doesn't clean up all this state
- A fresh CoValueCore starts with clean internal state
- Only preserves what we need: ID + lastKnownState + lastKnownStateSource

### 6. Updated `loadingState` Getter

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

The getter is updated to handle the new states with correct priority ordering:

```typescript
get loadingState() {
  if (this.verified) {
    return "available";
  }

  // Check for pending peers FIRST - loading takes priority over other states
  for (const peer of this.loadingStatuses.values()) {
    if (peer.type === "pending") {
      return "loading";
    }
  }

  // Check for lastKnownStateSource (garbageCollected or onlyKnownState)
  if (this.#lastKnownStateSource) {
    return this.#lastKnownStateSource;
  }

  if (this.loadingStatuses.size === 0) {
    return "unknown";
  }

  for (const peer of this.loadingStatuses.values()) {
    if (peer.type === "unknown") {
      return "unknown";
    }
  }

  return "unavailable";
}
```

**Important:** The `pending` check comes FIRST. This ensures that if a `garbageCollected` or `onlyKnownState` CoValue starts loading from a peer, it transitions to `"loading"` state correctly.

### 7. Updated `knownState()` Method

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

The `knownState()` method is updated to use `#lastKnownState` when the CoValue is in `garbageCollected` or `onlyKnownState` state. This simplifies the API - callers can always use `knownState()` to get the best known state.

```typescript
/**
 * Returns the known state of the CoValue.
 * 
 * The return value identity is going to be stable as long as the CoValue is not modified.
 * On change the knownState is invalidated and a new object is returned.
 * 
 * For garbageCollected/onlyKnownState CoValues, returns the #lastKnownState.
 */
knownState(): CoValueKnownState {
  // 1. If we have verified content in memory, use that (authoritative)
  if (this.verified) {
    return this.verified.immutableKnownState();
  }
  
  // 2. If we have last known state (GC'd or onlyKnownState), use that
  if (this.#lastKnownState) {
    return this.#lastKnownState;
  }
  
  // 3. Fallback to empty state (truly unknown CoValue)
  return emptyKnownState(this.id);
}
```

**Benefits of this approach:**
- Single method to get known state - no need for separate accessor methods
- Callers don't need special handling for different states
- Semantic meaning: "knownState" = "best knowledge we have about this CoValue"
- Simpler code in `startPeerReconciliation`, `internalLoadFromPeer`, etc.

### 8. Cleanup When Transitioning to Available

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

When a `garbageCollected` or `onlyKnownState` CoValue becomes fully available, we need to clean up the cached state to avoid memory waste.

```typescript
/**
 * Clean up lastKnownState when CoValue becomes available.
 * Called after the CoValue transitions from garbageCollected/onlyKnownState to available.
 */
private cleanupLastKnownState() {
  // Clear both fields - in-memory verified state is now authoritative
  this.#lastKnownStateSource = undefined;
  this.#lastKnownState = undefined;
}
```

**Where to call it:** In `markFoundInPeer` after the CoValue becomes available:

```typescript
markFoundInPeer(peerId: PeerID, previousState: string) {
  this.loadingStatuses.set(peerId, { type: "available" });
  this.updateCounter(previousState);
  this.scheduleNotifyUpdate();
  
  // Clean up if transitioning from garbageCollected/onlyKnownState
  if (this.isAvailable()) {
    this.cleanupLastKnownState();
  }
}
```

**Why this matters:**
- Without cleanup, `#lastKnownState` stays in memory unnecessarily
- Proper cleanup ensures clean state transitions and predictable behavior

### 9. Modified `getKnownStateFromStorage` Method

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

The existing `getKnownStateFromStorage` method (from lazy-storage-load spec) is modified to set `onlyKnownState` state when it finds data:

```typescript
/**
 * Lazily load only the knownState from storage without loading full transaction data.
 * If found, marks the CoValue as onlyKnownState and caches the knownState.
 */
getKnownStateFromStorage(done: (knownState: CoValueKnownState | undefined) => void) {
  if (!this.node.storage) {
    done(undefined);
    return;
  }

  // If already available in memory or have last known state, return knownState()
  // (knownState() handles both cases - verified content or lastKnownState)
  const state = this.loadingState;
  if (this.isAvailable() || state === "garbageCollected" || state === "onlyKnownState") {
    done(this.knownState());
    return;
  }

  // Delegate to storage - caching is handled at storage level
  this.node.storage.loadKnownState(this.id, (knownState) => {
    if (knownState) {
      // Cache the knownState and mark as onlyKnownState
      const previousState = this.loadingState;
      this.#lastKnownStateSource = "onlyKnownState";
      this.#lastKnownState = knownState;
      this.updateCounter(previousState);
    }
    done(knownState);
  });
}
```

**Key behaviors:**
1. If available in memory → `knownState()` returns verified state
2. If already `garbageCollected` or `onlyKnownState` → `knownState()` returns `lastKnownState` (no storage lookup)
3. Otherwise → loads from storage, caches result, sets `onlyKnownState` via `#lastKnownStateSource`

This creates a natural integration point: any code path that calls `getKnownStateFromStorage` (e.g., server handling LOAD requests) will automatically mark the CoValue as `onlyKnownState`, making it eligible for subscription restoration on reconnect.

### 10. Updated `startPeerReconciliation` Method

**Location:** `packages/cojson/src/sync.ts`

The key change: for `garbageCollected` and `onlyKnownState` CoValues, send LOAD with `lastKnownState` instead of empty state. Since `knownState()` now returns `lastKnownState` when available, the code is simplified.

```typescript
startPeerReconciliation(peer: PeerState) {
  if (isPersistentServerPeer(peer)) {
    this.resumeUnsyncedCoValues().catch((error) => {
      logger.warn("Failed to resume unsynced CoValues:", error);
    });
  }

  const coValuesOrderedByDependency: CoValueCore[] = [];

  const seen = new Set<string>();
  const buildOrderedCoValueList = (coValue: CoValueCore) => {
    // ... unchanged ...
  };

  for (const coValue of this.local.allCoValues()) {
    const state = coValue.loadingState;

    if (coValue.isAvailable()) {
      // In memory - build ordered list for dependency-aware sending
      buildOrderedCoValueList(coValue);
    } else if (state === "unknown") {
      // Skip unknown CoValues - we never tried to load them, so don't
      // restore a subscription we never had. This prevents loading
      // content for CoValues we don't actually care about.
      continue;
    } else if (!peer.loadRequestSent.has(coValue.id)) {
      // For unavailable/loading/errored states:
      // knownState() returns empty state
      peer.trackLoadRequestSent(coValue.id);
      this.trySendToPeer(peer, {
        action: "load",
        ...coValue.knownState(),
      });
    }

    // Fill the missing known states with empty known states
    if (!peer.getKnownState(coValue.id)) {
      peer.setKnownState(coValue.id, "empty");
    }
  }

  // Available CoValues - send in dependency order (unchanged)
  for (const coValue of coValuesOrderedByDependency) {
    peer.trackLoadRequestSent(coValue.id);
    this.trySendToPeer(peer, {
      action: "load",
      ...coValue.knownState(),
    });
  }
}
```

**Simplified behavior (thanks to updated `knownState()`):**
- `available` → `knownState()` returns in-memory verified state
- `garbageCollected` or `onlyKnownState` → `knownState()` returns `lastKnownState` (the key optimization!)
- `unknown` → **SKIPPED** (no LOAD sent - we never cared about this CoValue)
- Other states (`unavailable`, `loading`, `errored`) → `knownState()` returns empty state

**Key insight:** Unknown CoValues are ones we only "heard about" (e.g., saw the ID in a reference) but never actually tried to load. Sending LOAD requests for them during reconciliation would cause unnecessary data transfer for content we don't actually need.

### 11. Updated `hasCoValueLoaded` in LocalNode

**Location:** `packages/cojson/src/localNode.ts`

The `hasCoValueLoaded` method must exclude `garbageCollected` and `onlyKnownState` states since they don't have actual content loaded:

```typescript
hasCoValueLoaded(id: RawCoID): boolean {
  const coValue = this.coValues.get(id);
  if (!coValue) {
    return false;
  }

  const state = coValue.loadingState;
  // garbageCollected and onlyKnownState shells don't have actual content loaded
  return (
    state !== "unknown" &&
    state !== "garbageCollected" &&
    state !== "onlyKnownState"
  );
}
```

### 12. Updated `loadFromStorageOrPeers` in CoValueCore

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

The method is updated to properly handle the new states:

```typescript
loadFromStorageOrPeers(peers: PeerState[], done?: (found: boolean) => void) {
  // ... storage check ...

  const currentState = this.loadingState;

  if (
    currentState !== "unknown" &&
    currentState !== "garbageCollected" &&
    currentState !== "onlyKnownState"
  ) {
    done?.(currentState === "available");
    return;
  }

  // ... continue with loading ...
}
```

**Why:** A `garbageCollected` or `onlyKnownState` CoValue should proceed to load from storage/peers since it doesn't have full content in memory.

### 13. Updated `loadFromPeers` in CoValueCore

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

The method is updated to allow loading from peers when state is `garbageCollected` or `onlyKnownState`:

```typescript
for (const peer of peers) {
  const currentState = this.getLoadingStateForPeer(peer.id);

  if (
    currentState === "unknown" ||
    currentState === "unavailable" ||
    currentState === "garbageCollected" ||
    currentState === "onlyKnownState"
  ) {
    this.markPending(peer.id);
    this.internalLoadFromPeer(peer);
  }
}
```

### 14. Updated `internalLoadFromPeer` (simplified)

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

When sending LOAD requests, `internalLoadFromPeer` now simply uses `knownState()` which handles all cases:

```typescript
if (!peer.closed) {
  // knownState() returns:
  // - verified state if available in memory
  // - lastKnownState if garbageCollected/onlyKnownState
  // - empty state otherwise
  peer.pushOutgoingMessage({
    action: "load",
    ...this.knownState(),
  });
  peer.trackLoadRequestSent(this.id);
}
```

**Why this works:** With the updated `knownState()` method, it automatically returns the `lastKnownState` for garbageCollected/onlyKnownState shells. No special handling needed.

### 15. Updated `internalLoadFromPeer` completion check

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

The subscription in `internalLoadFromPeer` needs to recognize `garbageCollected` and `onlyKnownState` as terminal states:

```typescript
if (
  state.isAvailable() ||
  peerState === "available" ||
  peerState === "errored" ||
  peerState === "unavailable" ||
  peerState === "garbageCollected" ||
  peerState === "onlyKnownState"
) {
  unsubscribe();
  removeCloseListener?.();
}
```

### 16. Updated `loadCoValue` in LocalNode

**Location:** `packages/cojson/src/localNode.ts`

The method is updated to treat `garbageCollected` and `onlyKnownState` as states that need loading:

```typescript
if (
  coValue.loadingState === "unknown" ||
  coValue.loadingState === "unavailable" ||
  coValue.loadingState === "garbageCollected" ||
  coValue.loadingState === "onlyKnownState"
) {
  const peers = this.syncManager.getServerPeers(id, skipLoadingFromPeer);
  // ... load from storage and peers ...
}
```

## Data Model

### New Fields in CoValueCore

```typescript
class CoValueCore {
  // Existing - tracks loading state from sources (storage, peers)
  private readonly loadingStatuses: Map<...>;
  
  // NEW: Tracks why we have lastKnownState (lifecycle state, not loading state)
  #lastKnownStateSource?: "garbageCollected" | "onlyKnownState";
  
  // NEW: Cached knownState for garbageCollected/onlyKnownState CoValues
  #lastKnownState?: CoValueKnownState;
}
```

### State Transitions

```
                    ┌─────────────┐
                    │   unknown   │ ← Initial state (just heard the ID)
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           │ getKnownState │               │ loadFromPeer/loadFromStorage
           │ FromStorage   │               │
           │ (found)       │               ▼
           │               │        ┌─────────────┐
           │               │        │   loading   │
           │               │        └──────┬──────┘
           │               │   ┌───────────┼───────────┐
           ▼               │   ▼           ▼           ▼
    ┌─────────────────┐    │ ┌─────────┐ ┌───────────┐ ┌─────────┐
    │ onlyKnownState │    │ │available│ │unavailable│ │ errored │
    └────────┬────────┘    │ └────┬────┘ └───────────┘ └─────────┘
             │             │      │
             │             │      │ GC unmount
             │ reload      │      ▼
             │             │ ┌───────────────────┐
             │             │ │ garbageCollected  │ ← Keeps lastKnownState
             │             │ └─────────┬─────────┘
             │             │           │
             │             │           │ reload
             ▼             ▼           ▼
           ┌─────────────────────────────┐
           │          available          │
           └─────────────────────────────┘
```

**Key transitions:**
- `unknown` → `onlyKnownState`: When `getKnownStateFromStorage()` finds data in storage
- `available` → `garbageCollected`: When GC unmounts a CoValue (caches knownState first)
- Both `onlyKnownState` and `garbageCollected` → `available`: When fully loaded/reloaded

### Reload Cycle for GarbageCollected CoValues

When a `garbageCollected` CoValue is reloaded, the **same shell CoValueCore is reused** (not replaced). Here's the detailed flow:

1. **GC unmounts the CoValue** (`unmount()` called):
   - Original CoValueCore is deleted via `internalDeleteCoValue()`
   - New shell CoValueCore is created via `createGarbageCollectedCoValue()`
   - Shell has: `#lastKnownStateSource = "garbageCollected"`, `#lastKnownState` set

2. **User requests the CoValue** (`loadCoValue()` or subscription):
   - `loadingState` is `"garbageCollected"` → triggers load from storage/peers
   - `loadFromPeers()` is called → marks peer as `pending`
   - `loadingState` transitions to `"loading"` (pending check takes priority)

3. **Content arrives from peer/storage**:
   - `handleNewContent()` processes transactions
   - `_verified` is populated with content
   - `markFoundInPeer()` is called

4. **Cleanup on becoming available** (`markFoundInPeer()`):
   - Sets peer status to `available`
   - Calls `cleanupLastKnownState()` which:
     - Clears `#lastKnownStateSource` (no longer needed)
     - Clears `#lastKnownState` (no longer needed)
   - `loadingState` now returns `"available"` (because `verified` is set)

5. **CoValue is fully available**:
   - Same CoValueCore instance, now with full content
   - Can be GC'd again later (clean cycle)

```
Shell CoValueCore                    After Reload
┌─────────────────────────────┐     ┌─────────────────────────────┐
│ _verified: null             │     │ _verified: {...}            │
│ #lastKnownStateSource: "gc" │ ──> │ #lastKnownStateSource: null │
│ #lastKnownState: {...}      │     │ #lastKnownState: null       │
│ loadingStatuses: (empty)    │     │ loadingStatuses:            │
│                             │     │   peer: "available"         │
└─────────────────────────────┘     └─────────────────────────────┘
```

**Important:** The shell is NOT replaced during reload. The same CoValueCore instance is populated with content. This is efficient because:
- No need to update references elsewhere
- Listeners attached to the shell continue to work
- The CoValue ID mapping in `node.coValues` stays stable

## Sequence Diagrams

### Scenario 1: GC'd CoValue Restoration

```
Client                    GC                      Server
  |                        |                          |
  |  [CoValue available]   |                          |
  |                        |                          |
  |  [no listeners]        |                          |
  |<-- unmount() ----------|                          |
  |                        |                          |
  |  [state = garbageCollected]                       |
  |  [lastKnownState saved: header/1]               |
  |                        |                          |
  |  [later: reconnect]    |                          |
  |                        |                          |
  |  [startPeerReconciliation]                        |
  |                        |                          |
  |  if garbageCollected:                             |
  |-- LOAD (sessions: header/1) -------------------->|
  |                        |   [compare knownStates]  |
  |                        |   [server also has       |
  |                        |    header/1, no diff]    |
  |<-- KNOWN (sessions: header/1) -------------------|
  |                        |                          |
```

**Before this change:** Client would send `LOAD (sessions: empty)`, causing server to send ALL content.

**After this change:** Client sends `LOAD (sessions: header/1)`, server responds with KNOWN (no data transfer needed).

### Scenario 2: Server-side `onlyKnownState` via getKnownStateFromStorage

```
Client                    Server                  Storage
  |                          |                        |
  |-- LOAD (knownState) ---->|                        |
  |                          |                        |
  |                          |  [CoValue not in memory]
  |                          |                        |
  |                          |-- getKnownStateFromStorage -->
  |                          |                        |
  |                          |<-- storageKnownState --|
  |                          |                        |
  |                          |  [state = onlyKnownState]
  |                          |  [lastKnownState saved]
  |                          |                        |
  |                          |  [compare knownStates] |
  |                          |                        |
  |<-- KNOWN (or CONTENT) ---|                        |
  |                          |                        |
  |  [later: server reconnects to upstream]           |
  |                          |                        |
  |                          |  [startPeerReconciliation]
  |                          |                        |
  |                          |  if onlyKnownState:   |
  |                          |-- LOAD (lastKnownState) ->
```

## Error Handling

### Missing lastKnownState

With the updated `knownState()` method, there's no need for explicit fallback handling. The method naturally handles all cases:

```typescript
knownState(): CoValueKnownState {
  if (this.verified) {
    return this.verified.immutableKnownState();
  }
  if (this.#lastKnownState) {
    return this.#lastKnownState;
  }
  return emptyKnownState(this.id);  // Graceful fallback
}
```

If `#lastKnownState` is somehow undefined for a `garbageCollected` CoValue (shouldn't happen), `knownState()` returns an empty state automatically. This degrades gracefully to the old behavior (server sends all content).

### Memory Considerations

Keeping CoValueCore instances in memory after GC uses minimal memory:
- Only the shell remains (no verified content)
- `#lastKnownState` is small (ID + header boolean + session counts)
- `#lastKnownStateSource` is a simple string
- Trade-off: small memory cost vs. preventing massive unnecessary loads

## Testing Strategy

### Implemented Tests

Tests are located in `packages/cojson/src/tests/sync.peerReconciliation.test.ts`:

#### 1. `sends lastKnownState for garbageCollected CoValues during reconciliation`

**Setup:**
- Client with storage and GC enabled
- Create group and map, sync to server
- Disconnect, then run GC to unmount CoValues

**Assertions:**
- `loadingState` is `"garbageCollected"` after GC
- `knownState()` returns the `lastKnownState` from before GC (not empty)
- On reconnect, LOAD is sent with last known sessions (e.g., `header/1`) not empty
- Server responds with KNOWN (no content transfer needed)

**Expected message flow:**
```
client -> server | LOAD Map sessions: header/1
client -> server | LOAD Group sessions: header/3
server -> client | KNOWN Group sessions: header/3
server -> client | KNOWN Map sessions: header/1
```

#### 2. `unknown CoValues are skipped during reconciliation`

**Setup:**
- Create a fresh client that only calls `getCoValue(id)` without loading (i.e., just heard about the ID)

**Assertions:**
- `loadingState` is `"unknown"`
- No LOAD messages are sent for this CoValue during peer reconciliation
- The CoValue ID is skipped entirely (not tracked in `loadRequestSent`)

**Rationale:** Unknown CoValues represent IDs we encountered (e.g., in references) but never actively tried to load. Sending LOAD requests for them during reconciliation would transfer unnecessary data for content we don't actually care about.

#### 3. `garbageCollected CoValues restore subscription with minimal data transfer`

**Setup:**
- Client and server both have the same CoValue data
- Client GCs the CoValue, then reconnects

**Assertions:**
- Client sends LOAD with `lastKnownState`
- Server responds with KNOWN (not CONTENT) since both have same data
- No actual content is transferred

### Updated Existing Tests

In `packages/cojson/src/tests/sync.garbageCollection.test.ts`:

The test for "syncing after GC runs" was updated to expect:
- `LOAD Map sessions: header/1` (last known state) instead of `LOAD Map sessions: empty`
- Server uses `GET_KNOWN_STATE` instead of full `LOAD` when responding

### Test File Locations

- Peer reconciliation tests: `packages/cojson/src/tests/sync.peerReconciliation.test.ts`
- GC-related tests: `packages/cojson/src/tests/sync.garbageCollection.test.ts`

## Implementation Status

The following components have been implemented:

| Component | Status | Location |
|-----------|--------|----------|
| `#lastKnownStateSource` field | ✅ Done | `coValueCore.ts` |
| `#lastKnownState` field | ✅ Done | `coValueCore.ts` |
| `loadingState` getter update | ✅ Done | `coValueCore.ts` |
| `knownState()` uses `#lastKnownState` | ✅ Done | `coValueCore.ts` |
| `setGarbageCollectedState()` method | ✅ Done | `coValueCore.ts` |
| `cleanupLastKnownState()` method | ✅ Done | `coValueCore.ts` |
| `unmount()` creates GC shell | ✅ Done | `coValueCore.ts` |
| `createGarbageCollectedCoValue()` | ✅ Done | `localNode.ts` |
| `hasCoValueLoaded()` excludes GC states | ✅ Done | `localNode.ts` |
| `loadCoValue()` handles GC states | ✅ Done | `localNode.ts` |
| `loadFromStorageOrPeers()` handles GC states | ✅ Done | `coValueCore.ts` |
| `loadFromPeers()` handles GC states | ✅ Done | `coValueCore.ts` |
| `internalLoadFromPeer()` uses `knownState()` | ✅ Done | `coValueCore.ts` |
| `getKnownStateFromStorage()` sets `onlyKnownState` | ✅ Done | `coValueCore.ts` |
| `startPeerReconciliation()` uses `knownState()` | ✅ Done | `sync.ts` |
| Unit tests | ✅ Done | `sync.peerReconciliation.test.ts` |

## Migration / Compatibility

This is a behavioral change that should be transparent to users:
- No API changes
- CoValues that were previously loaded unnecessarily will no longer be loaded
- Existing subscriptions continue to work normally

## Future Considerations

### Persistent Subscription Tracking

This design solves two in-session problems:
1. **GC'd values** - CoValues that were in memory but got garbage collected
2. **PartiallyLoaded values** - CoValues where we checked storage but didn't fully load

For cross-restart persistence:
- Could persist `garbageCollected` and `onlyKnownState` CoValue IDs to storage
- On restart, read the list and restore CoValueCores with their last known states
- This would enable true subscription restoration across app restarts

### Memory Pressure

If many CoValues are GC'd or onlyKnownState but kept as shells:
- Could implement a second-level GC that removes very old shells
- Or limit the number of entries with last known states
- Priority should be given to `garbageCollected` (actively used) over `onlyKnownState` (only checked)

### State Cleanup

When a `onlyKnownState` or `garbageCollected` CoValue becomes fully `available`:
- Clear `#lastKnownStateSource` (no longer needed)
- Clear `#lastKnownState` (no longer needed, in-memory state is authoritative)

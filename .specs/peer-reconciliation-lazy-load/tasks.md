# Implementation Tasks (v2)

This is the updated task list for the new approach using explicit `garbageCollected` and `onlyKnownState` states.

## Phase 1: Core State Changes

- [x] **Task 1**: Add new states to `loadingStatuses` type in `CoValueCore`
  - Location: `packages/cojson/src/coValueCore/coValueCore.ts`
  - Add `"garbageCollected"` and `"onlyKnownState"` to the type union
  - Add `lastKnownState?: CoValueKnownState` field to store the knownState

- [x] **Task 2**: Update `loadingState` getter to handle new states
  - Location: `packages/cojson/src/coValueCore/coValueCore.ts`
  - Check storage status for `garbageCollected` or `onlyKnownState`
  - Return the appropriate state string
  - **Note:** `pending` check comes first so loading state is correctly detected

- [x] **Task 3**: Update `knownState()` method to use `lastKnownState`
  - Location: `packages/cojson/src/coValueCore/coValueCore.ts`
  - Returns `lastKnownState` for `garbageCollected` or `onlyKnownState` CoValues
  - Returns empty state for truly unknown CoValues

- [x] **Task 4**: Modify `getKnownStateFromStorage` to set `onlyKnownState`
  - Location: `packages/cojson/src/coValueCore/coValueCore.ts`
  - When storage returns a knownState:
    - Set `lastKnownState = knownState`
    - Set storage status to `{ type: "onlyKnownState" }`
    - Call `updateCounter(previousState)`
  - Also returns last known state if already in `garbageCollected` or `onlyKnownState`

- [x] **Task 5**: Add `cleanupLastKnownState()` method for transition to available
  - Location: `packages/cojson/src/coValueCore/coValueCore.ts`
  - Create private method that:
    - Checks if storage status is `garbageCollected` or `onlyKnownState`
    - Clears `lastKnownState` (set to undefined)
    - Deletes the storage status entry from `loadingStatuses`
  - Call this method in `markFoundInPeer` when `isAvailable()` returns true

## Phase 2: GC Changes

- [x] **Task 6**: Add `createGarbageCollectedCoValue` method to `LocalNode`
  - Location: `packages/cojson/src/localNode.ts`
  - Create a new CoValueCore with the given ID
  - Call `setGarbageCollectedState` with the last known state
  - Add it to the `coValues` map

- [x] **Task 7**: Add `setGarbageCollectedState` method to `CoValueCore`
  - Location: `packages/cojson/src/coValueCore/coValueCore.ts`
  - Set `lastKnownState` to the provided value
  - Set storage status to `{ type: "garbageCollected" }`
  - Call `updateCounter(null)` to track metrics

- [x] **Task 8**: Modify `unmount()` to delete and replace with garbageCollected shell
  - Location: `packages/cojson/src/coValueCore/coValueCore.ts`
  - Cache `knownState()` before deleting
  - Call `internalDeleteCoValue` to remove the old CoValueCore
  - Call `node.createGarbageCollectedCoValue(id, lastKnownState)` to create the shell
  - **Note:** `storage?.onCoValueUnmounted` is called within `internalDeleteCoValue`

## Phase 3: Reconciliation Changes

- [x] **Task 9**: Update `startPeerReconciliation` to use `lastKnownState` for GC'd CoValues
  - Location: `packages/cojson/src/sync.ts`
  - Check `loadingState` for each CoValue:
    - `"available"` → build ordered list (existing behavior)
    - `"garbageCollected"` or `"onlyKnownState"` → send LOAD with `lastKnownState`
    - Other states → send LOAD with empty state (existing behavior)

- [x] **Task 10**: Update related loading methods
  - `loadFromStorageOrPeers()` handles `garbageCollected`/`onlyKnownState`
  - `loadFromPeers()` handles `garbageCollected`/`onlyKnownState`
  - `internalLoadFromPeer()` recognizes new states as terminal
  - `loadCoValue()` in LocalNode treats new states as needing load
  - `hasCoValueLoaded()` excludes new states (no actual content loaded)

## Phase 4: Testing

- [x] **Task 11**: Peer reconciliation tests for garbageCollected CoValues
  - Location: `packages/cojson/src/tests/sync.peerReconciliation.test.ts`
  - Test: `sends lastKnownState for garbageCollected CoValues during reconciliation`
  - Test: `unknown CoValues are skipped during reconciliation`
  - Test: `garbageCollected CoValues restore subscription with minimal data transfer`

- [x] **Task 12**: Update existing GC tests
  - Location: `packages/cojson/src/tests/sync.garbageCollection.test.ts`
  - Updated snapshot expectations for new behavior (lastKnownState sent instead of empty)

---
"cojson": patch
---

Optimized peer reconciliation to prevent unnecessary data transfer on reconnect.

**Key improvements:**
- Added new CoValue states `garbageCollected` and `onlyKnownState` to distinguish between CoValues we actively used versus ones we just heard about
- Garbage-collected CoValues now cache their `knownState` before unmounting, enabling diff-only sync on reconnect instead of full content transfer
- Unknown CoValues (IDs we encountered but never loaded) are now skipped during peer reconciliation, preventing unnecessary LOAD requests
- The `knownState()` method now returns the cached state for GC'd/onlyKnownState CoValues, simplifying sync logic across the codebase
- When `getKnownStateFromStorage()` finds data, it marks the CoValue as `onlyKnownState` making it eligible for subscription restoration
- Refactored unmount logic into `LocalNode.internalUnmountCoValue()` for efficient single-operation map updates when creating GC shells

**Before:** Client reconnects → sends LOAD with empty state for all CoValues → server sends ALL content for every ID ever heard of

**After:** Client reconnects → sends LOAD with cached knownState for GC'd CoValues → server sends only the diff; unknown CoValues are skipped entirely

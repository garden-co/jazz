---
"jazz-tools": patch
---

Add a sync status API. `db.getSyncStatus()` returns the upstream connection (`local-only`, `connecting`, `connected`, or `offline` with a reason) and how many committed transactions the server has not accepted or rejected yet, plus a `synced` flag. `db.onSyncStatusChange(listener)` reports changes, and `db.waitForSync({ timeoutMs })` resolves once every earlier write reached the server, waiting through outages like a `global` write wait.

The frameworks get the same status: `useSyncStatus()` for React, React Native, Solid and Vue, `syncStatus()` for Svelte, and a `syncStatus` stream plus `waitForSync` in the Effect layer.

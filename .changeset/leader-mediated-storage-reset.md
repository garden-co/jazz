---
"jazz-tools": patch
---

Browser persistent `deleteClientStorage()` (and `logout({ wipeData: true })`) now coordinates with other tabs before removing the OPFS namespace.

Previously, a follower tab calling `deleteClientStorage()` would only shut down its own bridge/follower port — the leader tab's dedicated worker kept the OPFS file open, so the subsequent removal could fail with `NoModificationAllowedError` or race a live writer and corrupt the recreated namespace.

The new flow broadcasts a `storage-reset-start` notification on `jazz:storage-reset:${appId}:${dbName}` so every tab's `Db` shuts down (terminating the leader's dedicated worker and releasing OPFS handles). The originating tab then holds the `jazz:leader:…` lock across `removeEntry` so no other tab's supervisor can promote itself mid-delete.

`deleteClientStorage()` remains terminal — the `Db` instance must be recreated to keep working after a wipe.

---
"jazz-tools": patch
---

Make `Db.deleteClientStorage()` retryable after a partial failure.

Before, the method gated on `!this.supervisor` to detect non-SharedWorker Db instances. But `runShutdown` (which `deleteClientStorage` calls as its second step) nulls `this.supervisor` mid-flight, so a call that succeeded at the broadcast + shutdown phase and then threw at the OPFS delete (e.g. the OS hadn't finished releasing the terminated leader worker's `FileSystemSyncAccessHandle` within the retry budget) left the Db in a state where every subsequent `deleteClientStorage()` early-returned with "is only available on browser SharedWorker-backed Db instances." The caller had no way to retry the deletion — they could only construct a fresh `Db`, which would then try to open the still-locked OPFS file and fail too.

The guard now uses the construction-time `usesSharedWorker` latch, which survives shutdown. The body skips the broadcast and shutdown on a retry (both are idempotent against the post-shutdown state, but reissuing them is wasted work and noise) and goes straight to `withLeaderLockHeld(removeBrowserStorageNamespace)`. That path depends only on `navigator.locks` and `navigator.storage`, not on the supervisor, so it works after teardown.

Net effect: if `await db.deleteClientStorage()` rejects with the OPFS-still-locked error, calling it again retries just the delete with a fresh retry budget.

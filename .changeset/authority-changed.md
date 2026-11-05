---
"jazz-tools": patch
---

A client whose server's store was recreated (a deleted dev data directory, a restarted in-memory server, a new environment behind the same URL) no longer stays silently stuck. The core refuses that server with a typed `authority_changed` error: `db.getSyncStatus().link` becomes `{ state: "offline", reason: "authority-changed" }`, remote subscriptions end with it, and `wait({ tier: "global" })` and `waitForSync()` fail fast with an `AuthorityChangedError` (detect it with `isAuthorityChangedError`) instead of hanging. Offline writes are kept locally and are no longer uploaded into the new server's history. Nothing is reset automatically: call `db.deleteClientStorage()` (or delete the client's data directory in Node) to sync with the new server. Servers now mint their authority namespace when the store is opened, so a backup taken before any client connected restores to the same authority.

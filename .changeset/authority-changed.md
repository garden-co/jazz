---
"jazz-tools": patch
---

A client whose server's store was recreated (a deleted dev data directory, a restarted in-memory server, a new environment behind the same URL) no longer stays silently stuck. The core refuses that server with a typed `authority_changed` error: `db.getSyncStatus().link` becomes `{ state: "offline", reason: "authority-changed" }`, remote subscriptions end with it, and `wait({ tier: "global" })` and `waitForSync()` fail fast with an `AuthorityChangedError` (detect it with `isAuthorityChangedError`) instead of hanging. Offline writes are kept locally and are no longer uploaded into the new server's history. Nothing is reset automatically: call `db.deleteClientStorage()` (or delete the client's data directory in Node) to sync with the new server. Servers now mint their authority namespace when the store is opened, so a backup taken before any client connected restores to the same authority.

Any upstream link the core refuses now stops sync instead of keeping a socket open that nothing syncs over: besides a changed authority, this covers a stranded store and a server catalogue this client cannot mirror (for example a column type mismatch). Those show as `{ state: "offline", reason: "stopped" }` and are retried only by `db.reconnect()`.

A client that reconnects with unsynced writes now uploads them after the server's welcome instead of with its join, which costs one extra round trip.

This is verified for clients that hold the server link themselves (Node and the core). In the browser, where tabs sync through a shared worker, the tab path is unverified: a tab's `global` waits are not yet told about a refusal its worker received and may keep waiting.

Rust: `jazz::db::ErrorCode` has a new variant, `AuthorityChanged`, so exhaustive `match`es on `ErrorCode` need a new arm.

---
"jazz-tools": patch
---

**Rust (`jazz-app`): a server serving another history is reported as `authority-changed`, as TypeScript reports it.** This happens when the server's store was recreated, or it's another app's server.

- **What the client does:** it stops syncing with that server. `jazz.sync_status()` reports `Offline { reason: AuthorityChanged, retrying: false }` and `last_error: AuthorityChanged`, serialized as `"authority-changed"`.
- **Local data:** local reads and writes keep working, and unsynced writes are kept. Nothing is reset automatically.
- **Resyncing with the new server:** delete the local store and open the client again.
- **Before:** the link stayed up while nothing synced over it.

In the `jazz` crate, `JazzClient::upstream_refusal()` says why the database stopped the link.

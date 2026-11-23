---
"jazz-tools": patch
---

Each end user of a persistent store directory now has a store of their own: Rust (`Storage::Dir`), Swift, Kotlin, Go and C (`dataDir`). Before, every user of one directory shared one store, so a user who signed in after another saw that user's data, and the earlier user's unsent writes were uploaded under the next user's identity.

- **Where:** an end user's store, and a scoped backend client's, is now in `<dir>/<account id>/`, as `JazzSession` already kept them. An app without a user, the app's backend and trusted backend clients keep theirs in the directory itself.
- **Upgrading: sync first.** A store an earlier version kept in the directory itself is left as it is, never read or deleted. A signed-in user who upgrades opens a new store and gets their synced data from the server, but writes that store hadn't uploaded stay behind in the old one. Before upgrading, wait for sync (`wait_for_sync`, `waitForSync`), or accept that those writes are lost. The old store's files can be deleted afterwards.
- **An app without a user** refuses a store in its directory that a signed-in user wrote (`Error::Refused`, kind `refused`): open it as that user, or delete it. One with only its own writes opens as before.
- **Fixed:** a user who signed in on a directory another user had used could have their own writes refused ("commit names a node another identity writes as"), because the two shared one client id.

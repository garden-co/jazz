---
"jazz-tools": patch
---

**Rust: `delete_client_storage()` leaves nothing behind on unix.** It used to leave an empty `jazz.lock` in the store directory; on Linux and macOS it now removes it last, while still holding it, as TS's `deleteClientStorage` leaves nothing. An app that opened the lock file just before then takes its lock again on the current file, so the guard against deleting a store another process has open still holds. On Windows the lock file stays, as before.

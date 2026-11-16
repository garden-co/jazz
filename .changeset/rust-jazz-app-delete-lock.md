---
"jazz-tools": patch
---

**Rust: `delete_client_storage()` leaves nothing behind.** It used to leave an empty `jazz.lock` in the store directory; it now removes it last, while still holding it, as TS's `deleteClientStorage` leaves nothing. An app that opened the lock file just before then takes its lock again on the current file, so the guard against deleting a store another process has open still holds.

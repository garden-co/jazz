---
"jazz-tools": patch
---

**Behaviour change (Rust, Swift, Kotlin, Go, C):** one app at a time can have a persistent store open. Opening a `dataDir` that another app has open, in the same process or another, now fails with `StoreHeld` (`refused` in the bindings). Two apps on one store never worked: writes failed with "database is locked", a client's sync stopped until it was reopened, and both could mint the same transaction id. Share the one handle, or give each app its own directory. An open that follows a close, sign-out or last drop in the same process waits up to 5 seconds for it. A failed open and every `shutdown` now return with the store released, and `deleteClientStorage` keeps the store locked until it is gone.

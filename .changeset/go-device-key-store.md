---
"jazz-tools": patch
---

**Fix (Go, experimental):** `FileStore` no longer loses a device account's key. An empty or blank key file, or a link to a missing one, is now an error from `Open` and is left as it is, where it was replaced by a new account's key; delete the file to start as a new account. Saves replace the file whole, so a crash can't leave it empty and its directory must be writable; saving an empty key is refused. Openers racing to create the account all act as the one that was kept (on file systems with hard links).

---
"jazz-tools": patch
---

A browser app whose IndexedDB runs out of storage space no longer loses writes or corrupts its local store. Before, every write still reported saved past the quota; then rows went missing, or the store reopened as "database disk image is malformed". Now the local store always holds a whole committed state: writes IndexedDB could not take stay in memory, keep syncing, and are saved once a retry succeeds. While saving fails, a write's `wait({ tier: "local" })` in a tab rejects with an error with code `storage` that names the cause. A tab's local wait now resolves only once the write is in IndexedDB, about 1–2 ms later than before.

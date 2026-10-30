---
"jazz-tools": patch
---

A live subscription waiting on cold storage no longer keeps graph garbage collection pending, so later polls stop repeating collection work that cannot reclaim anything.

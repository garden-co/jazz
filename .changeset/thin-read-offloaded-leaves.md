---
"jazz-tools": patch
---

A thin client reading a large value whose bytes live only in an `s3()` (or other network) blob store, for example after the server restarted, no longer hangs the server. The read now fetches the value's chunks from the store and answers; before, it waited for a fetch the busy server could never finish, and every later request timed out until a restart.

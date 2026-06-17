---
"jazz-tools": patch
---

Fix a crash when syncing a row with a long edit history. Forwarding a row's parent batches to a server walked the history recursively, so a row with a deep history chain (a few hundred edits is enough on a browser worker's stack) could overflow the stack — surfacing as "memory access out of bounds" on the client and out-of-memory on the server. The walk is now iterative and visits each parent batch at most once.

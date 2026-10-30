---
"jazz-wasm": patch
"jazz-tools": patch
---

In the browser, reads of data already in memory no longer wait while a write commits to IndexedDB. Outside a transaction they return the last committed state: the in-flight write becomes visible once its commit succeeds, and a failed commit never does. Inside a transaction, reads still see the transaction's own writes. Reads that need data not yet in memory still wait for the commit. The IndexedDB page format is unchanged.

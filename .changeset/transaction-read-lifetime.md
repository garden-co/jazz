---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Keep a transaction's reads inside its lifetime. Reads run in the order they were started, see the writes staged before them, and share one serializable snapshot. Committing while a read started through the transaction is still pending now fails with a `not_observed` error and ends the transaction without publishing it, instead of publishing before the read answered. Await every transaction read before committing.

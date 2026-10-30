---
"jazz-tools": patch
---

Add `jazz-tools/thin`, a thin client that asks the server for query results instead of syncing a local replica: no WASM, worker or local storage, the same `Db` API and React hooks, and self-signed sign-in in plain JavaScript. Writes, transactions, branch and snapshot views and large-value streaming all run on the server; offline writes are not supported in thin mode yet.

---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Keep local writes visible while a query waits for large-value bytes from the server. A write no longer waits for another query's first result that is stuck on an attachment fetch, for example while offline. That query restarts after the write and keeps its pending fetch.

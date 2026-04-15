---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-rn": patch
"jazz-napi": patch
---

Ship the new unified row-history storage engine across Jazz runtimes, and expose the new replayable write/batch APIs in TypeScript.

Relational rows, query visibility, and sync replay now go through the same storage-backed path instead of mixing durable state with older in-memory cache layers. In practice this makes local persistence and sync behavior more consistent across browser, Node, and native runtimes, especially around cold start, reconnect, and large local datasets.

This release also adds the high-level replayable write handles and explicit batch builders that sit on top of that storage model:

- `DbPersistedWrite` / `PersistedWrite`
- `DbTransaction`
- `DbDirectBatch`

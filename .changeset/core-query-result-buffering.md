---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Move subscription result buffering into the Rust core and remove redundant TypeScript query preparation and row caches. Preserve scoped relation reads and wait for complete recursive query results before publishing them. Direct binding consumers must adapt to removal of the Rust–TypeScript `prepareQuery` API.

[PR #2717](https://github.com/garden-co/jazz/pull/2717).

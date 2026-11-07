---
"jazz-wasm": patch
---

Shrink the browser WASM by about 13% gzip: release builds optimise the engine for size, drop the random mangling hash from kept function names and let wasm-opt fold pointer offsets. IndexedDB reads can be up to 7% slower; writes are unaffected.

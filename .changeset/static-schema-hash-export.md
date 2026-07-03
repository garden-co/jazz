---
"jazz-wasm": patch
"jazz-tools": patch
---

Expose `WasmRuntime.computeSchemaHash` as a static WASM export so schema hashes can be computed without constructing a throwaway runtime.

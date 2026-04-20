---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Modify write APIs to return a `WriteHandle`, which allows callers to wait for a given durability tier to acknowledge the write or reject it. Also introduces a global `onMutationError` handler to receive errors that aren't explicitly handled with `WriteHandle.wait`.

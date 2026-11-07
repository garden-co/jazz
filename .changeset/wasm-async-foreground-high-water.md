---
"jazz-tools": patch
"jazz-wasm": patch
---

Read the foreground transaction-time high-water asynchronously in the browser runtime. The synchronous WASM call blocked on the node lock while the lock holder waited for the JS event loop, so closing a foreground runtime could spin at full CPU instead of completing.

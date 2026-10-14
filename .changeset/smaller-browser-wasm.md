---
"jazz-wasm": patch
---

Shrink the browser WASM by about 5% gzip with no runtime slowdown: release builds drop the random mangling hash from kept function names and let wasm-opt fold pointer offsets.

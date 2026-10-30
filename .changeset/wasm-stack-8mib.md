---
"jazz-wasm": patch
---

Reserve an 8 MiB stack for the WebAssembly module, matching native builds, so deeply nested queries no longer overflow the stack in development builds.

---
"jazz-tools": patch
---

Fix writes and queries failing under `vite dev` on Vite 6 and 7 with `optimizeDeps: { exclude: ["jazz-wasm"] }`. The local-first client started, but every insert and query then failed with `CompileError: WebAssembly.instantiate(): expected magic word 00 61 73 6d`. The dev server had pre-bundled Jazz's browser worker too, so the worker loaded index.html as its WASM. Jazz now asks the dev server for the worker as a worker entry, which it serves from the package's own files.

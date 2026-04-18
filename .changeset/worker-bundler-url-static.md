---
"jazz-tools": patch
---

Use static `new URL(...)` import for worker when no explicit runtime sources are configured, allowing bundlers (Turbopack, webpack, Vite) to detect and co-bundle the worker script and its WASM dependency automatically.

Also passes a computed `fallbackWasmUrl` in the worker init message so non-bundled (static HTML) deployments still receive an explicit WASM path as a last resort if `wasmModule.default()` fails.

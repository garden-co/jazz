---
"jazz-tools": patch
---

`jazzPlugin` and `jazzSvelteKit` now inject `build.target: "es2020"`, `worker.format: "es"`, and `optimizeDeps.exclude: ["jazz-wasm"]` via a Vite `config` hook. Consumers no longer need to set these manually in their `vite.config.ts`. The `es2020` target is required because `jazz-wasm` exports a `u64` function via wasm-bindgen, which generates BigInt code — bundlers targeting older environments will attempt to downcompile it and break the WASM bindings.

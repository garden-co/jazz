---
"jazz-tools": patch
---

`jazzPlugin` and `jazzSvelteKit` now inject `worker.format: "es"` and `optimizeDeps.exclude: ["jazz-wasm"]` via a Vite `config` hook. Consumers no longer need to set these manually in their `vite.config.ts`.

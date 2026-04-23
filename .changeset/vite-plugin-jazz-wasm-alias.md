---
"jazz-tools": patch
---

`jazzPlugin` and `jazzSvelteKit` now alias `jazz-wasm` to an absolute path resolved from `jazz-tools`'s own install location. This removes the need for Vite/SvelteKit consumers on pnpm to add `jazz-wasm` as a direct dependency just to work around pnpm's strict-isolation layout.

---
"jazz-wasm": patch
---

Declare `"type": "module"` in `package.json` so Node treats the ESM output from `wasm-pack --target web` as ESM without content-sniffing. Fixes `ERR_REQUIRE_CYCLE_MODULE` when the `jazz-tools` CLI runs under an ESM loader hook (e.g. `tsx`) installed over a flat npm/npx/bunx node_modules layout.

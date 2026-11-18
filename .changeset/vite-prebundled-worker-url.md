---
"jazz-tools": patch
---

The local-first client now starts under `vite dev` on Vite 6 and 7 with the documented `optimizeDeps: { exclude: ["jazz-wasm"] }`. Those versions pre-bundle jazz-tools and pointed its browser worker at a path the dev server answered with index.html, so the client stayed thin unless `jazz-tools` was excluded too. When Jazz runs from Vite's dependency pre-bundle, it now has the dev server load the worker from the `jazz-tools` package. Keeping `jazz-tools` in `optimizeDeps.exclude` still works. Other bundlers and production builds are unchanged.

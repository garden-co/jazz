---
"jazz-tools": patch
---

The local-first client now fails at once with a `BrowserWorkerScriptLoadError` when its browser worker script doesn't load, instead of staying thin after a silent 10 s timeout. The error names the cause: under `vite dev` on Vite 6 and 7, pre-bundled jazz-tools points the worker at a path the dev server answers with index.html. To fix that, add `"jazz-tools"` to `optimizeDeps.exclude` next to `"jazz-wasm"`, or upgrade to Vite 8. For a `runtimeSources` worker URL, the error says what the server answered (for example, an HTML page).

---
"jazz-tools": patch
---

Vite builds no longer contain an unused, unminified copy of the browser broker worker (174 KB gzip in `dist/`). With `runtimeSources.wasmUrl` and `wasmVersion` but no `brokerWorkerUrl`, the page now starts the worker its bundler built; the version still keeps each deployment's tabs in their own worker.

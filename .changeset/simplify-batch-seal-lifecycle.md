---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
---

Persist sealed batch manifests and batch fates instead of replayable local batch records. Batch waits and mutation-error replay now read `BatchFate` directly, and sync no longer rebuilds local batch membership one row at a time.

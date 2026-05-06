---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
---

Replace replayable batch settlements with whole-batch `BatchFate` sync semantics and remove visible-member manifests from the client-facing fate shape. Successful fate now applies by batch id to locally known rows, avoiding repeated per-row member decoding during subscription settlement.

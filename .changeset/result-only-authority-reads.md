---
"@jazz/rust": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-tools": patch
---

Add an opt-in `resultOnly` mode for flat Global one-shot reads. Eligible reads return the admitted authority's result without waiting to populate the local offline cache; ordinary reads retain their existing materialization behavior.

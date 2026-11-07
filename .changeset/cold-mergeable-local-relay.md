---
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Load cold foreground mergeable transaction reads from their durable local relay,
even while its server is offline. Preserve staged writes, cancellation and
mergeable conflict semantics instead of returning an empty foreground cache.

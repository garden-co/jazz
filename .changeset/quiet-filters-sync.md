---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
"create-jazz": patch
---

Fix `in` filters inside included relation queries, including empty lists returning no included rows. This bumps the sync protocol for the new `Condition` enum variants.

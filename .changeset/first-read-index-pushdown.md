---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Answer first one-shot reads with fewer row loads: point joins by an exact id use the junction's foreign-key index, root and join equality filters are pushed into indexes, and two equality filters intersect their index keys before rows are loaded. Live subscriptions are unchanged.

---
"jazz-tools": patch
"jazz-wasm": patch
---

Gate tiered browser subscriptions so the first callback is held until the worker bridge has replayed the settled server snapshot instead of exposing an empty transient snapshot.

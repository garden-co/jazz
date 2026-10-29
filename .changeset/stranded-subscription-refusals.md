---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
"jazz-rn-android": patch
"jazz-rn-ios": patch
---

Report `StrandedStore` when a subscription cannot sync because the store committed writes before becoming a replica. Preserve this reason across reconnects and reopening the store without exposing server diagnostic text.

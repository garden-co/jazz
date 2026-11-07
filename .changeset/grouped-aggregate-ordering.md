---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
"jazz-rn-ios": patch
"jazz-rn-android": patch
---

Fix grouped aggregate results using opaque identities for default ordering and explicit-order ties. Reads and live subscriptions now use logical group keys before pagination, while preserving group occurrence identities and explicit sort directions.

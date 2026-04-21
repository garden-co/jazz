---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Cache runtime schema lookups across query paths and surface unhandled rejected mutations through targeted batch-id queues instead of rescanning every retained local batch record. This also brings the React Native runtime onto the same rejected-batch helper surface as WASM and N-API.

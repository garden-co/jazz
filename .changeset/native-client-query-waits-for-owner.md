---
"jazz-tools": patch
---

Fix the native Rust `JazzClient` panicking with `synchronous node operation … reentered a suspended operation` when a query, subscription or transaction read started while the client was waiting on storage or large-value chunks. It now waits for that operation to yield and then runs. The NAPI, WASM and React Native bindings were not affected.

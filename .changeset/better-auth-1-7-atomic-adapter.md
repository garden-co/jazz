---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
---

Support Better Auth 1.7 in the Jazz database adapter with atomic `consumeOne` and
`incrementOne` operations. Exclusive transactions now preserve trusted-serving identities and
support identity-aware transaction reads across the native and WASM runtimes.

---
"jazz-tools": patch
---

Harden runtime sync outbox handling across WASM/RN and NAPI callback contracts by typing both callback shapes, routing both through a shared normalizer, and adding conformance tests that assert identical `/sync` behavior.

---
"jazz-tools": patch
"jazz-napi": patch
---

`db.waitForSync()` now rejects when a write committed before the call was refused, even if the refusal arrived before the call and later writes were accepted. Before, it resolved in that case, because it only waited for the writes still pending when called (jazz#5728). Each refusal is reported once, and one that the write's own `wait({ tier: "global" })` already reported isn't reported again. The same fix applies to Rust's `jazz.wait_for_sync()` and the native bindings.

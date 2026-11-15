---
"jazz-tools": patch
"jazz-napi": patch
---

`db.waitForSync()` now rejects when a write committed before the call was refused, even if the refusal arrived before the call and later writes were accepted. Before, it resolved in that case, because it only waited for the writes still pending when called (jazz#5728). Each refusal is reported once, and one that the write's own `wait({ tier: "global" })` already reported isn't reported again. A refusal goes only to its writer's wait: on a backend, a `forReqUser` or `forSession` Db's `waitForSync()` reports that user's refusals and the backend's own `db` reports its own (before, a backend Db's `waitForSync()` didn't wait at all). Graceful shutdown doesn't fail on a refusal that settled before it. The same fix applies to Rust's `jazz.wait_for_sync()`.

---
"jazz-tools": patch
---

Set `lifecycle: "hibernate"` on `durableObjects()` to let an idle Jazz core sleep while Cloudflare keeps its WebSockets open. Live subscriptions resume on wake.

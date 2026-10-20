---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Reuse bounded, source-independent compiler templates when subscriptions close and later reopen. Logical query state and live graph ownership are still released, and fresh reads rebind their inputs and permissions.

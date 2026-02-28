---
"jazz-tools": patch
---

Add a high-level server-side `createJazzContext` API in `jazz-tools/backend` with lazy runtime setup from generated app DSL objects, plus request/session-scoped helpers (`forRequest`, `forSession`) and lifecycle helpers (`flush`, `shutdown`).

---
"jazz-tools": patch
---

Simplify `jazz-tools/backend` to expose only the high-level `Db` and `createJazzContext` APIs. `JazzContext` no longer exposes a low-level `client()` escape hatch, and the backend entrypoint no longer re-exports low-level runtime client and transport internals.

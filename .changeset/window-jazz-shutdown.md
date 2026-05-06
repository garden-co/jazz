---
"jazz-tools": patch
---

Add `window.__jazz.shutdown(namespace?)` for awaiting Jazz client teardown (worker termination + OPFS lock release). Useful in browser tests that mount and unmount apps between cases. `Db.shutdown()` is now idempotent — concurrent or repeated calls share the same in-flight promise — so the new API plays cleanly alongside framework-driven cleanup (e.g. JazzProvider unmount).

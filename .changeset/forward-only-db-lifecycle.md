---
"jazz-tools": patch
---

Give `Db` and the native runtime a lifecycle phase that only moves forward. A graceful shutdown that can't sync its writes (an offline sign-out) now leaves the tab's worker connection recovering, so the Db it leaves open keeps working; once a Db's teardown starts it stays closed, and operations on it fail with an explicit error naming its phase. On React Native, `close()` after `deleteClientStorage()` now really tears the runtime down and releases its native handle.

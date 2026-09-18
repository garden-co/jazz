---
"jazz-tools": patch
---

Yield database owner ticks while a retained operation or external read holds the node lock, and wake the owner when an external read releases it. This prevents stalled owner queues without busy-loop scheduling or losing deferred cleanup.

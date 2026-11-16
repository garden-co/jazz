---
"jazz-tools": patch
---

A read issued right after a large burst of writes no longer times out while the server is still working through them. The server answers a read after the writes made before it, so a backend's first `jazz.db` read after 50,000 inserts waited for all of them, and could fail with "Timed out waiting for query coverage". Two changes fix this. The server now applies a burst of one writer's commits in time linear in its size: matching the rows it sends back to the writer's commits had cost N² comparisons. A read's 15 s coverage budget also restarts each time one of the writes made before it settles, so it fails only when the server stops making progress, not because the server is busy. Writes made after the read don't restart it, and a read waits at most 150 s in total.

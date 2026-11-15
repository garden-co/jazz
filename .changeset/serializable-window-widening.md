---
"jazz-tools": patch
---

A serializable transaction no longer misses a conflict on a row its own staged write brought into a `limit`, `offset` or `one()` read. For example: mark job A done, take the next queued job with `one()`, get B, claim B. Before, the read was recorded as `[A]`, so a concurrent claim of B went undetected and B could be claimed twice. Now the read is recorded, and checked by the server, with its window widened by the number of rows the transaction wrote to that table (and started earlier when it has an offset), so it covers every row the staged writes could have moved into it. Relation windows widen the same way. A windowed read whose rows depend on other rows the transaction wrote (a join, a required include or relation, or a read policy that reads other rows) now fails with a "not supported in serializable transactions yet" error instead of committing unchecked.

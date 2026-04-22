---
"jazz-tools": patch
---

Treat reconnect row-history replay as idempotent only when the incoming row exactly matches the stored history member. This avoids spuriously reclassifying replayed inserts as updates on insert-only tables while still allowing same-batch corrections to propagate their final payload.

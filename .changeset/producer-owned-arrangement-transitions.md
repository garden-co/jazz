---
"jazz-tools": patch
---

Maintain shared join indexes at their producer and consume read-only transitions, avoiding full-bucket publication snapshots and unchanged-row replay in semi/anti-joins.

---
"jazz-tools": patch
---

Every server now numbers commit positions from a hybrid clock (wall-clock milliseconds plus a counter) instead of counting up by one, so an unsharded server runs the same code as one shard of a sharded deployment. Positions, write receipts' sequence numbers and `$transactionId` values are therefore large and not dense. They still only increase, and an existing store continues above what it numbered.

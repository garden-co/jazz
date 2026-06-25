---
"jazz-tools": patch
---

Unify batches and transactions to transaction kinds. `Db.transaction(callback)` and `Db.beginTransaction()` now create "mergeable" transactions (previously `Db.batch` and `Db.beginBatch`), while `Db.exclusiveTransaction(callback)` and `Db.beginExclusiveTransaction()` (previously `Db.transaction(callback)` and `Db.beginTransaction()`) create serializable exclusive transactions. Write handles also expose `transactionId` instead of `batchId`.

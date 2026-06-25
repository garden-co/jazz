---
"jazz-tools": patch
---

Unify batches and transactions as two different kinds of transactions:

- `Db.transaction(callback)` and `Db.beginTransaction()` now create "mergeable" transactions (previously `Db.batch` and `Db.beginBatch`), while `Db.exclusiveTransaction(callback)` and `Db.beginExclusiveTransaction()` (previously `Db.transaction(callback)` and `Db.beginTransaction()`) create serializable exclusive transactions.
- Write handles expose `transactionId` instead of `batchId`.
- The `.wait()` method on exclusive transactions does not expect a durability tier, since it resolves when the authority approves the transaction.

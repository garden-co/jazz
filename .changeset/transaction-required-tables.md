---
"jazz-tools": patch
---

Add schema support for transaction-required tables in `jazz-tools`.

You can now mark a table as requiring transactional writes with either `defineTable(...).requireTransaction()` or `table(..., columns, { requiresTransaction: true })`. When a table is marked this way, direct writes like `db.insert(...)`, `db.update(...)`, `db.delete(...)`, and direct batches now fail immediately with a clear error, while transactional writes continue to work normally. Non-transactional tables are still allowed inside transactions.

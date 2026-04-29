---
"jazz-tools": patch
---

- Transactions and batches no longer require a table when being created. The schema is determined by the first CRUD operation performed inside the transaction.
- Add new `Db.transaction` and `Db.batch` methods. These methods receive a callback as parameter and commit automatically once the callback finishes running. They are preferred over their `beginTransaction`/`beginBatch` counterparts.

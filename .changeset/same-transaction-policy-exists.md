---
"jazz-tools": patch
---

A permission policy's `exists` now sees earlier writes of the same transaction when the server checks it. Creating a row and the rows whose permissions depend on it in one `db.transaction(...)` or exclusive transaction is accepted instead of rejected with `permission_denied`. Rows the transaction deletes, and writes from other transactions that are not accepted yet, still never satisfy `exists`.

---
"jazz-tools": patch
---

A permission policy's `exists` now sees the rows the same transaction inserts or updates when the server checks it. Creating a row and the rows whose permissions depend on it in one `db.transaction(...)` or exclusive transaction is accepted instead of rejected with `permission_denied`. A committed row the transaction deletes still satisfies `exists`; a row the transaction both inserts and deletes, and writes from other transactions that are not accepted yet, never do.

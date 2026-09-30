---
"jazz-tools": patch
---

A permission policy's `exists` now sees the rows the same transaction inserts or updates when the server checks it. Creating a row and the rows whose permissions depend on it in one `db.transaction(...)` or exclusive transaction is accepted instead of rejected with `permission_denied`. A committed row the transaction deletes still satisfies `exists`; a row the transaction both inserts and deletes, and writes from other transactions that are not accepted yet, never do. Every write must also pass against the state the whole transaction leaves behind, so a transaction that removes the access one of its own writes relies on is rejected. A transaction whose checks would read more than 262,144 of its own rows is rejected as not supported yet (`malformed_commit`).

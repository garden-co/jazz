---
"jazz-tools": patch
---

The update and delete policy checks that judge the rows a transaction acts on now read committed state wherever the transaction is decided, so their outcome no longer depends on the order of the transaction's writes.

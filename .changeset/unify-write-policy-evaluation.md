---
"jazz-tools": patch
---

Transaction write policies are now evaluated by one procedure on every path that decides a transaction, so the outcome no longer depends on whether the transaction was staged at the server or committed by a client and uploaded.

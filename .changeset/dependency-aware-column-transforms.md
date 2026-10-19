---
"jazz-tools": minor
---

Add optional `dependsOn` declarations to column transforms. Read callbacks receive stored dependency values; write callbacks receive app-facing dependency values, with missing values fetched automatically. Selections keep their public shape, and subscriptions recompute when dependencies change.

Transform callback or dependency lookup failures prevent an explicit transaction from committing, even if the application catches the error. A caught synchronous input validation error before staging does not, by itself, invalidate the transaction.

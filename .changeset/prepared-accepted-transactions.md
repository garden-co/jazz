---
"jazz-tools": patch
---

Keep typed asynchronous serializable preparation attached to its transaction.
Commit waits for registered reads and writes. Paired accepted reads retain the
selected owner and read session; callback scopes reject after rollback or client
close, including evidence returned after retirement.

---
"jazz-tools": patch
---

Queries inside a transaction read each table's history once instead of re-scanning every row's history, so each stored version is decoded once rather than three times. Transactional reads and checkouts are noticeably faster. Results are unchanged.

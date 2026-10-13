---
"jazz-tools": patch
---

Exclusive transactions now record what a count or other aggregate returned instead of every row it counted. A commit that counts many rows stays small, and it is no longer rejected when another writer changes a counted row without changing the count, sum, minimum or maximum the transaction saw. It is still rejected with `exclusive_conflict` when that result changes. Reading an aggregate after writing in the same exclusive transaction now fails with an "is not supported in exclusive transactions yet" error; read the aggregate before the first write.

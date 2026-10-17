---
"jazz-tools": patch
---

The Jazz server now reads the query indexes of stored tables from storage by key and keeps only a bounded cache of them in memory, instead of holding every indexed row. The cache's budget is a quarter of the container memory limit (or `JAZZ_QUERY_CACHE_BUDGET_BYTES`). When resident memory passes 80% of the limit, the budget steps down gradually, and queries read more rows from storage. It stops lowering when that does not free memory, and grows back once memory frees up. Set `JAZZ_STORED_ARRANGEMENTS=0` to hold the indexes in memory as before. Clients are unchanged and keep holding their indexes.

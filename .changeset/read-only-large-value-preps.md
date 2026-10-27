---
"jazz-tools": patch
---

Queries that filter with `contains` on, compare or order by large string, JSON or bytes values no longer write to the database while they run. A query that would have to compare more than about 6,000 large values at once now fails with a "not supported yet" error instead of running.

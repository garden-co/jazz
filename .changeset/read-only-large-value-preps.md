---
"jazz-tools": patch
---

Queries that filter with `contains` on, compare, order or group by large string, JSON or bytes values no longer write to the database while they run. A query that would have to answer more than about 2 MB of such comparisons at once now fails with a "not supported yet" error instead of running.

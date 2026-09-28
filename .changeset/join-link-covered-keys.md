---
"jazz-tools": patch
---

Queries that join through a link table with a two-column composite index on that table (for example `[linkColumn, targetColumn]`) now skip link rows whose target is outside the root's indexed prefix, instead of hydrating every link row first. Results are unchanged.

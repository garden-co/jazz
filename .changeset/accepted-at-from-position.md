---
"jazz-tools": patch
---

`$acceptedAt` in history reads is now always the time the server accepted a version, taken from the version's position on the server. Before, it copied `$updatedAt`, so a write that set `updatedAt` explicitly (a backfill or an import) was listed at that time, and history ordered by `$acceptedAt` didn't end with the current row. `$updatedAt` still keeps the time you set.

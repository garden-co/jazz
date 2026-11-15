---
"jazz-tools": patch
---

`$acceptedAt` in history reads is now always the time the server accepted a version, taken from the version's position on the server. Before, it copied `$updatedAt`, so a write that set `updatedAt` explicitly (a backfill or an import) was listed at that time, and history ordered by `$acceptedAt` didn't end with the current row. `$updatedAt` still keeps the time you set.

**Apps imported from alpha.59:** the importer replays each version with its original `updatedAt`. Those versions now show `$acceptedAt` as the time the import wrote them, and their original times remain in `$updatedAt`. Only pre-release development stores from before alpha.60 hold positions that aren't server clock times; their `$acceptedAt` is wrong, and they aren't supported.

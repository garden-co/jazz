---
"jazz-tools": patch
---

When two migration paths between the same schema versions give different results, the refusal now names the table and column and how the paths differ (for example `for `todos.priority`, one path adds it with default 0 and the other adds it with default 1`), with the versions as `migrations/snapshots` names them, instead of two chains of full hashes. `jazz-tools validate` now refuses such paths, two different migrations between the same versions, and a snapshot whose contents don't match its hash, as a deploy does.

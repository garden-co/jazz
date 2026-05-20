---
"jazz-tools": patch
---

`Query.hopTo("relation")` now infers the destination table's row type instead of carrying the source table's type through. Consumers extracting the row type via `s.RowOf<>` (or any other consumer of the query builder's row type) now see the destination shape.

The returned handle intentionally drops destination-table chain methods (`where`, `select`, `include`, `orderBy`, `limit`, `offset`, `requireIncludes`, `hopTo`) because the runtime still serializes those clauses against the source table at `_build()` time. The handle keeps `_build`, `_serializeRelation`, and `gather` so the existing hop-then-traverse pattern (`app.x.where(...).hopTo(rel).gather(...)`) and `db.all(...) / db.one(...) / db.subscribeAll(...)` continue to work.

---
"jazz-tools": patch
---

Relation queries now return exactly their projected columns under the requested aliases, with the source column's type and nullability, on both one-shot and live reads, instead of whole source rows. `include(...)` and `select(...)` keep working when the projection is the table's full identity (as for `where` with payload `match`); combining them with a renaming or narrowing projection, ordering a `union(...)` by a column outside its output table or by `$createdBy`/`$updatedBy`, and aliases that reuse internal names (`row_uuid`, `tx_time`, `tx_node_id`, or a `$`, `_app_`, `__`, `left.` or `right.` prefix) are now rejected with a clear error.

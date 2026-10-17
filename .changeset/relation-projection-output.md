---
"jazz-tools": patch
---

Relation queries whose projection renames or narrows columns now return exactly the projected columns under the requested aliases, with each source column's type and nullability, on both one-shot and live reads, instead of whole source rows. Queries whose projection is the table's full column set (as `hopTo(...)` and payload `match` queries produce) keep returning ordinary rows and keep supporting `include(...)` and `select(...)`. Combining `include(...)` or `select(...)` with a renaming or narrowing projection, ordering a `union(...)` by a column outside its output table or by `$createdBy`/`$updatedBy`, and using an internal name (`row_uuid`, `tx_time`, `tx_node_id`, or a `$`, `_app_`, `__`, `left.` or `right.` prefix) as an output alias in a renaming projection or in any `union(...)`, including a `union(...)` over a table with such a column, are now rejected with a clear error.

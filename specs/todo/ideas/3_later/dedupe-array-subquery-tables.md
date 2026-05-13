# Dedupe array_subquery_tables entries

## What

`QueryGraph.array_subquery_tables` is a `Vec<(NodeId, TableName)>` populated by `compile.rs` and consumed by `involves_table` / `mark_dirty_for_table`. The list can carry duplicate `(node_id, table)` pairs — already possible pre-existing, and more likely now that nested array subqueries register their tables against the outer node. Consumers tolerate duplicates (idempotent dirty bits, short-circuit `.iter().any`), so the cost is a few wasted bool writes per mutation. Worth deduping defensively to keep the list small and make consumer code easier to reason about.

## Notes

Options:

- Dedupe at registration in `compile.rs` (`.iter().any(...)` check before push — O(N) per push, fine for small N).
- Dedupe at consumption in `execute.rs` by collecting affected node ids into a `HashSet` before iterating.

Either is a self-contained change with no behaviour shift. Same applies to `policy_filter_tables` / `magic_column_tables` / `recursive_relation_tables` if they have similar shape.

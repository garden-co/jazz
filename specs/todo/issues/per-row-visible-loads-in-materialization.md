# Per-row visible-region loads during query materialization

## What

`MaterializeNode::materialize_tuple` calls `QueryManager::load_visible_row_for_query` once per tuple, and each call walks `load_best_visible_row_batch_with_hint_or_locator` → `load_visible_query_row_from_candidate_tables` → row-locator load + `raw_table_get` + decode against durable storage. On OPFS-backed workers this is one btree point-lookup chain per row.

Profiled on the `todo-react` stress dataset (Trace-20260610T123151): ~1.0s of a 3.2s reload is spent in this path on the worker thread — `load_visible_region_row` alone is ~850ms, with `load_row_locator` ~376ms and `resolved_row_tables_for_table` ~253ms of repeated per-call table resolution inside it. This is independent of batch settlement: it is the steady-state cost of materializing a large subscription from storage.

## Priority

high — it is the dominant remaining cost of large-dataset reloads now that retained-batch replay and the `add_server` row sweep are fixed.

## Notes

- Where:
  - `crates/jazz-tools/src/query_manager/graph_nodes/materialize.rs` (`materialize_tuple`)
  - `crates/jazz-tools/src/query_manager/manager.rs` (`load_visible_row_for_query`, `load_best_visible_row_batch_with_hint_or_locator`, `load_visible_query_row_from_candidate_tables`)
  - `crates/jazz-tools/src/storage/mod.rs` (`load_visible_region_row_bytes_with_storage`, `resolved_row_tables_for_table`, `common_case_exact_visible_row_table_locator`)
- Likely shape of the fix: batch the loads — materialize from a visible-region range scan (the rows are contiguous per table/branch) instead of N point lookups, and/or cache `resolved_row_tables_for_table` per (table, schema) for the duration of one materialization pass.
- Related: [oversized-visible-row-storage](oversized-visible-row-storage.md), [row-storage-common-case-encoding](row-storage-common-case-encoding.md).

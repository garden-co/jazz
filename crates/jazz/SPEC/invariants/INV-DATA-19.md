# INV-DATA-19

- Status: now
- Coverage: ✓

## Invariant

`jazz_global_changes` MUST be keyed by `(physical_table_id, branch_key, row_uuid, layer, global_time)` and expose global-time and physical-table/global-time indexes.

## Enforced by (tests)

`jazz::schema::tests::global_changes_table_key_and_index_match_sync_contract`; `jazz::node::tests::catalogue_lenses::global_changes_span_table_renames_for_history_and_conflict_detection`

## Implementation

`jazz/src/schema.rs::global_changes_table`

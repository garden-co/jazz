# INV-LOWER-8

- Status: now
- Coverage: ✓

## Invariant

`jazz_global_changes` MUST use physical table identity and expose bounded global-time access paths.

## Enforced by (tests)

`jazz::schema::tests::global_changes_table_key_and_index_match_sync_contract`; `jazz::node::tests::catalogue_lenses::global_changes_span_table_renames_for_history_and_conflict_detection`

## Implementation

`jazz/src/schema.rs::global_changes_table`; `jazz/src/node/global_state.rs::NodeState::global_currency_changed_after`

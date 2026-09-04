# INV-QUERY-11

- Status: now
- Coverage: ✓

## Invariant

Local/unsettled query reads MUST return rows complete only relative to node-local visible-current knowledge.

## Enforced by (tests)

`jazz::node::tests::db_facade_current_rows_match_seeded_create_delete_sequence`; `jazz::node::tests::db_facade_multi_row_query_matches_seeded_create_delete_sequence_via_write_handles`

## Implementation

`node/query_eval.rs::NodeState::query_rows_with_prepared_plan`; `db.rs::Db::read`; `db.rs::Db::all`

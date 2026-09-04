# INV-LOWER-15

- Status: planned
- Coverage: [#1777](https://github.com/garden-co/jazz/issues/1777)

## Invariant

Whole-table current-row sync views MUST be represented as the normal table-rooted row-set shape, not a separate current-row serving engine; their result set must match the node's lowered `current_rows` result while migration code still exists.

## Enforced by (tests)

`jazz::node::tests::queries::view_update_result_set_matches_groove_current_rows_for_seeded_commits`

## Implementation

`jazz/src/node/query_engine/mod.rs::RowSetNormalizer`; `jazz/src/node/views.rs::NodeState::view_update_for_current_rows`; `jazz/src/node/tests/support.rs::assert_view_update_result_set_matches_current_rows`

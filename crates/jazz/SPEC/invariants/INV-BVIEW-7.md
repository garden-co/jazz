# INV-BVIEW-7

- Status: now
- Coverage: ✓

## Invariant

A table with no branch columns MUST behave as shared data in every branch view.

## Enforced by (tests)

`jazz::tests::branch_views::branch_view_join_projects_branch_column_subsets_and_shared_tables`; `jazz::node::tests::harness::branch_view_selects_head_then_base_and_keeps_unbranched_tables_shared`

## Implementation

`schema.rs::JazzSchema::project_branch_view_selector`; `node/query_eval/query_read_sets.rs::query_read_set_for_read_view`

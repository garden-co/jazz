# INV-BVIEW-9

- Status: now
- Coverage: ✓

## Invariant

An overlay MUST select head winners before base winners independently for content and deletion layers, and MUST perform that masking before predicates or relational operators.

## Enforced by (tests)

`jazz::tests::branch_views::indexed_branch_view_masks_base_before_applying_the_predicate`; `jazz::node::tests::harness::branch_view_selects_head_then_base_and_keeps_unbranched_tables_shared`

## Implementation

`node/query_eval/read_sources.rs::CurrentQuerySourceResolver::resolve_source`; `node/source_resolution.rs::NodeState::branch_view_rows_for_schema`

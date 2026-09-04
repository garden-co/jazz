# INV-QUERY-29

- Status: now
- Coverage: ✓

## Invariant

Every array subquery in a structured result MAY specify a finite child `limit` (zero is valid); omission selects the complete ordered suffix after `offset`. Logical result size MUST NOT be constrained by a transport frame limit: transport decomposition and reassembly MUST preserve the complete logical message atomically.

## Enforced by (tests)

`jazz::tests::structured_result_tree::{nested_tree_preserves_projection_order_offset_and_reset, omitted_array_limit_is_unbounded_for_prepare_read_and_subscribe, large_parent_is_materialized_atomically_without_a_frame_bound}`

## Implementation

`jazz/src/query.rs::validate_array_subquery`; `jazz/src/node/query_eval.rs::normalize_array_subquery`; transport fragmentation and reassembly

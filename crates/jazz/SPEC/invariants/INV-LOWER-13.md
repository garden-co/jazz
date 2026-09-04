# INV-LOWER-13

- Status: now
- Coverage: ✓

## Invariant

Aggregation, ordinary read ordering, general pagination, and projection MUST be applied by the node after row materialization, not required from groove lowering, except maintained unordered `limit(1)` with offset `0` which MAY lower through groove `ArgMinBy` over `row_uuid`, and maintained ordered windows or ordered suffixes which MUST lower through groove `TopBy`.

## Enforced by (tests)

`jazz::node::tests::queries::node_finishes_aggregation_ordering_pagination_and_projection_after_materialization`; `jazz::peer::tests::maintained_subscription_view_limit_one_installs_subscription`; `jazz::peer::tests::maintained_subscription_view_limit_one_switches_after_winner_delete_and_lower_insert`; `jazz::peer::tests::maintained_subscription_view_order_by_asc_limit_two_initial_hydration`; `jazz::peer::tests::maintained_subscription_view_order_by_asc_limit_two_boundary_insert_delete_updates`; `jazz::peer::tests::maintained_subscription_view_order_by_offset_limit_uses_top_by_window`; `jazz::peer::tests::maintained_subscription_view_order_by_without_limit_matches_one_shot_order`; `jazz::peer::tests::maintained_subscription_view_order_by_offset_without_limit_matches_one_shot_window`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::finish_query_rows`; `jazz/src/node/query_eval.rs::lower_query_graph`; `jazz/src/node/query_eval.rs::apply_maintained_view_result_limit`; `jazz/src/node/query_eval.rs::NodeState::ensure_maintained_view_query_slice`

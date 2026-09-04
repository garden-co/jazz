# INV-RLS-13

- Status: now
- Coverage: ✓

## Invariant

Historical/as-of reads served for a link MUST evaluate read policy at the requested historical cut.

## Enforced by (tests)

jazz::node::tests::time_travel::query_rows_at_for_link_evaluates_read_policy_at_historical_cut

## Implementation

jazz/src/node/query_eval.rs::NodeState::query_rows_at_for_link; jazz/src/node/query_eval.rs::NodeState::policy_composed_shape_binding

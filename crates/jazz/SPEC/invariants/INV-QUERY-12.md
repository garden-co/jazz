# INV-QUERY-12

- Status: now
- Coverage: ✓

## Invariant

Settled query reads on a subscriber MUST be answerable from the subscription's settled subscription result set; unresolvable result-set entries are an invariant violation rather than a degraded answer.

## Enforced by (tests)

`jazz::peer::tests::incremental_query_result_set_drops_enter_then_leave_same_drain_cycle`; `jazz::peer::tests::incremental_query_result_set_keeps_leave_then_reenter_same_drain_cycle`

## Implementation

`node/query_eval.rs::NodeState::query_rows_with_prepared_plan`; `node/query_eval.rs::NodeState::query_rows_from_result_set`

# INV-REC-6

- Status: now
- Coverage: ✓

## Invariant

A recursive fixpoint MUST stop when the accepted frontier is empty and MUST converge on cyclic inputs by deduplicating against accumulated facts.

## Enforced by (tests)

`recursive_cycle_regressions::incremental_ticks_converge_on_cycles`; `recursive_cycle_regressions::recompute_converges_on_cycles_at_subscribe`; `recursive_cycle_regressions::retraction_recompute_converges_while_a_cycle_exists`; `groove::db::tests::recursive_graph_subscriptions_converge_on_self_cycles`

## Implementation

`groove/src/ivm/runtime/recursion.rs::recursive_delta`; `groove/src/ivm/runtime/recursion.rs::recompute_recursive`

# INV-TICK-16

- Status: prov
- Coverage: ✓

## Invariant

The reference implementation selects recompute for negative table deltas, cached recursive state with table deltas, empty unbound state, or unhydrated step arrangements. This trigger set is broader than the minimum necessary; the contractual result is the minimal diff required by INV-REC-8.

## Enforced by (tests)

groove::db::tests::recursive_graph_subscriptions_retract_derived_paths_after_delete; groove::db::tests::recursive_graph_subscriptions_recompute_after_edge_update; groove::tests::recursive_cycle_regressions::retraction_recompute_converges_while_a_cycle_exists

## Implementation

groove/src/ivm/runtime/recursion.rs::recursive_delta; groove/src/ivm/runtime/recursion.rs::RecursiveState::replace_with

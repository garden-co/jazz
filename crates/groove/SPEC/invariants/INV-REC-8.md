# INV-REC-8

- Status: now
- Coverage: ✓

## Invariant

Retractions reaching recursive state MUST be handled by full recompute from storage and diff against the previous accumulated set; subscribers MUST receive only the resulting net recursive delta.

## Enforced by (tests)

`groove::db::tests::recursive_graph_subscriptions_retract_derived_paths_after_delete`; `groove::db::tests::prepared_recursive_binding_retracts_transitive_paths_after_edge_delete`; `groove::db::tests::prepared_recursive_binding_retracts_paths_after_first_edge_delete`; `groove::db::tests::recursive_graph_subscriptions_recompute_after_edge_update`; `recursive_cycle_regressions::retraction_recompute_converges_while_a_cycle_exists`

## Implementation

`groove/src/ivm/runtime/recursion.rs::recursive_delta`; `groove/src/ivm/runtime/recursion.rs::RecursiveState::replace_with`; `groove/src/ivm/runtime/recursion.rs::recompute_recursive`

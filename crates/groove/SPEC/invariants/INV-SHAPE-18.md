# INV-SHAPE-18

- Status: now
- Coverage: ✓

## Invariant

Prepared recursive shapes MUST route retractions caused by base-table deletes or anti-join changes to the correct bound subscriber result.

## Enforced by (tests)

groove::db::tests::prepared_recursive_binding_retracts_transitive_paths_after_edge_delete; groove::db::tests::prepared_recursive_binding_retracts_paths_after_first_edge_delete; groove::db::tests::prepared_recursive_binding_retracts_transitive_paths_from_antijoin_input; groove::db::tests::prepared_recursive_binding_retracts_first_paths_from_antijoin_input

## Implementation

groove/src/ivm/runtime/mod.rs::TickEvaluator::update_recursive; groove/src/ivm/runtime/mod.rs::route_shape_records; groove/src/ivm/runtime/recursion.rs::recompute_recursive

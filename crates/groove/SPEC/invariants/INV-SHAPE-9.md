# INV-SHAPE-9

- Status: now
- Coverage: ✓

## Invariant

A prepared binding's materialized snapshot MUST be maintained as a weighted multiset where deltas that bring a record weight to zero remove that record.

## Enforced by (tests)

groove::db::tests::parameterized_shape_uses_set_semantics_with_duplicate_param_refcounts; groove::db::tests::prepared_recursive_binding_retracts_transitive_paths_after_edge_delete

## Implementation

groove/src/ivm/runtime/mod.rs::route_shape_records; groove/src/ivm/runtime/mod.rs::PreparedBindingState

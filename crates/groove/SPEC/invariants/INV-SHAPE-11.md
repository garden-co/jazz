# INV-SHAPE-11

- Status: now
- Coverage: ✓

## Invariant

Binding retractions discovered via dropped receivers during notification MUST be queued, then drained before subsequent user table/binding deltas and before prepare/bind hydration snapshots.

## Enforced by (tests)

groove::prepared_binding_regressions::dropped_shape_receiver_cleanup_retracts_binding_before_rebind; groove::prepared_binding_regressions::pending_retraction_does_not_corrupt_freshly_hydrated_sibling_shape

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::tick_with_params; groove/src/ivm/runtime/mod.rs::IvmRuntime::prepare; groove/src/ivm/runtime/mod.rs::IvmRuntime::bind_shape; groove/src/ivm/runtime/mod.rs::IvmRuntime::binding_snapshot_deltas

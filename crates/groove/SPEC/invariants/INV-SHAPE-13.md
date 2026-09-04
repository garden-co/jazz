# INV-SHAPE-13

- Status: now
- Coverage: ✓

## Invariant

During shape graph hydration, `BindingSource` nodes in `ArrangementUpdateMode::Replace` MUST read current binding snapshots, not pending/incremental binding deltas.

## Enforced by (tests)

groove::prepared_binding_regressions::second_identical_shape_does_not_wipe_existing_bindings; groove::prepared_binding_regressions::pending_retraction_does_not_corrupt_freshly_hydrated_sibling_shape

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::hydrate_shape_graph; groove/src/ivm/runtime/mod.rs::NodeState::update_binding_source; groove/src/ivm/runtime/mod.rs::IvmRuntime::binding_snapshot_deltas

# INV-SHAPE-12

- Status: now
- Coverage: ✓

## Invariant

Preparing an identical shape over an already-active binding source MUST NOT replace shared arrangements with an empty binding snapshot or otherwise wipe existing bindings.

## Enforced by (tests)

groove::prepared_binding_regressions::second_identical_shape_does_not_wipe_existing_bindings

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::prepare; groove/src/ivm/runtime/mod.rs::IvmRuntime::hydrate_shape_graph; groove/src/ivm/runtime/join.rs::ArrangementState::apply_update

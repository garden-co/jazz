# INV-SHAPE-17

- Status: now
- Coverage: ✓

## Invariant

A `BindingSource` tick in normal accumulate mode MUST emit only `BindingDelta`s whose `shape` matches the source's `BindingSourceOp.shape` and whose descriptor matches the node output.

## Enforced by (tests)

`groove::db::tests::parameterized_shape_hydrates_and_routes_by_param`

## Implementation

groove/src/ivm/runtime/mod.rs::NodeState::update_binding_source

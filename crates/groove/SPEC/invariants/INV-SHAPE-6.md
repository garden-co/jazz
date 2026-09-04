# INV-SHAPE-6

- Status: now
- Coverage: ✓

## Invariant

Binding a key whose refcount transitions 0 -> 1 MUST inject exactly one `+1` `BindingDelta` in a table-delta-free tick before serving the subscriber snapshot.

## Enforced by (tests)

`groove::db::tests::parameterized_shape_hydrates_and_routes_by_param`

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::bind_shape; groove/src/ivm/runtime/mod.rs::IvmRuntime::add_binding_ref; groove/src/ivm/runtime/mod.rs::IvmRuntime::tick_with_params

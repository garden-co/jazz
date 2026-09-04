# INV-QUERY-19

- Status: now
- Coverage: ✓

## Invariant

`BindingSourceOp` MUST NOT be evaluated through ordinary subscription/query graphs outside prepared shapes.

## Enforced by (tests)

`groove::db::tests::binding_sources_are_rejected_outside_prepared_shapes`

## Implementation

`groove/src/ivm/runtime/mod.rs::IvmRuntime::subscribe` (via `builder_contains_binding_source`) and `groove/src/ivm/runtime/mod.rs::NodeState::update_binding_source`

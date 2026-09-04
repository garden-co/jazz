# INV-SHAPE-15

- Status: now
- Coverage: ✓

## Invariant

Binding values MUST conform to the prepared shape's `binding_descriptor`; mismatched type/arity MUST fail before subscription hydration.

## Enforced by (tests)

groove::db::tests::prepared_subscription_validates_named_bindings

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::bind_shape; groove/src/records (via `RecordDescriptor::create`)

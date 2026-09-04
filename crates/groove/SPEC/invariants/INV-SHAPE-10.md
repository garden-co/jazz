# INV-SHAPE-10

- Status: now
- Coverage: ✓

## Invariant

Unsubscribing a shape subscription MUST decrement the binding refcount and MUST inject a `-1` binding delta only when the last reference is removed.

## Enforced by (tests)

groove::db::tests::parameterized_shape_uses_set_semantics_with_duplicate_param_refcounts

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::unsubscribe_shape_subscription; groove/src/ivm/runtime/mod.rs::IvmRuntime::remove_binding_ref

# INV-SHAPE-7

- Status: now
- Coverage: ✓

## Invariant

Binding an already-active key MUST NOT inject another binding-source delta, and MUST serve the new subscriber from the per-key materialized snapshot.

## Enforced by (tests)

groove::db::tests::parameterized_shape_uses_set_semantics_with_duplicate_param_refcounts

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::bind_shape; groove/src/ivm/runtime/mod.rs::IvmRuntime::shape_materialized_snapshot; groove/src/ivm/runtime/mod.rs::IvmRuntime::add_binding_ref

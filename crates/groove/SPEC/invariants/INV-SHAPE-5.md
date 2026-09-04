# INV-SHAPE-5

- Status: now
- Coverage: ✓

## Invariant

A binding source weighted record set MUST expose set semantics: for each active `BindingKey`, evaluation snapshots contain exactly one row with weight `+1`, regardless of subscriber refcount.

## Enforced by (tests)

groove::db::tests::parameterized_shape_uses_set_semantics_with_duplicate_param_refcounts

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::add_binding_ref; groove/src/ivm/runtime/mod.rs::IvmRuntime::binding_snapshot_deltas

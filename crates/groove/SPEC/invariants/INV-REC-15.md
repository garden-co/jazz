# INV-REC-15

- Status: now
- Coverage: ✓

## Invariant

Nested recursive graphs MUST be rejected during validation/compilation.

## Enforced by (tests)

`groove::db::tests::recursive_graphs_reject_nested_recursion_for_v0`

## Implementation

`groove/src/ivm/runtime/mod.rs::IvmRuntime::add_dedup_graph`; `groove/src/ivm/runtime/mod.rs::builder_contains_recursive`

# INV-REC-13

- Status: now
- Coverage: ✓

## Invariant

`arg_max_by` MUST NOT be accepted inside recursive graph seed or step graphs.

## Enforced by (tests)

`groove::db::tests::arg_max_by_rejects_unsupported_inputs_and_bad_primary_keys`

## Implementation

`groove/src/ivm/runtime/mod.rs::IvmRuntime::add_dedup_graph`; `groove/src/ivm/runtime/mod.rs::builder_contains_arg_max_by`

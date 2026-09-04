# INV-SHAPE-4

- Status: now
- Coverage: ✓

## Invariant

Graph-level `prepare` MUST reject any `output_key_fields` entry absent from the graph output descriptor.

## Enforced by (tests)

`groove::db::tests::graph_level_prepare_rejects_output_key_fields_not_in_output_descriptor`

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::prepare

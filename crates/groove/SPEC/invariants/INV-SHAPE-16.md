# INV-SHAPE-16

- Status: now
- Coverage: ✓

## Invariant

Prepared shapes MUST retain their output graph nodes while the shape remains registered.

## Enforced by (tests)

`groove::db::tests::prepared_shapes_retain_output_graph_nodes_without_subscribers`

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::prepare; groove/src/ivm/runtime/mod.rs::IvmRuntime::retained_node_ids

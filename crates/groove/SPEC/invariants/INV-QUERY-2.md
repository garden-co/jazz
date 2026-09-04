# INV-QUERY-2

- Status: now
- Coverage: ✓

## Invariant

A `NodeDescriptor` MUST validate operator input arity, input/output descriptor compatibility, join key arity, and field-index bounds before the runtime accepts the node.

## Enforced by (tests)

`groove::ivm::graph::tests::validation_rejects_join_key_arity_mismatches`

## Implementation

`groove/src/ivm/graph.rs::NodeDescriptor::validate`

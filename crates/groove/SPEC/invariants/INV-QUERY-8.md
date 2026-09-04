# INV-QUERY-8

- Status: now
- Coverage: ✓

## Invariant

An inner `JoinOp` MUST require equal-length left and right key vectors.

## Enforced by (tests)

`groove::ivm::graph::tests::validation_rejects_join_key_arity_mismatches`

## Implementation

`groove/src/ivm/graph.rs::NodeDescriptor::validate`; `groove/src/ivm/runtime/join.rs::JoinState::apply`

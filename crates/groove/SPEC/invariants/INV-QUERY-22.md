# INV-QUERY-22

- Status: now
- Coverage: ✓

## Invariant

`OpType::SemiJoin`, `OpType::Distinct`, `OpType::Negate`, and `OpType::Aggregate` MUST NOT be advertised as executable query operators until runtime support exists.

## Enforced by (tests)

`groove::ivm::runtime::tests::unsupported_query_operator_variants_are_not_executable`

## Implementation

`groove/src/ivm/runtime/mod.rs::TickEvaluator::update_node`

# INV-QUERY-16

- Status: now
- Coverage: ✓

## Invariant

SQL prepared-shape lowering MUST accept only equality predicates of the form `column = $parameter` or `$parameter = column` as binding predicates.

## Enforced by (tests)

`groove::ivm::planner::tests::rejects_non_equality_parameter_predicates`

## Implementation

`groove/src/ivm/planner.rs::Planner::lower_binding_predicate`

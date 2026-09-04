# INV-QUERY-15

- Status: now
- Coverage: ✓

## Invariant

SQL `plan_query` MUST reject query parameters; parameterized SQL MUST go through `plan_prepared_shape`/prepared binding flow.

## Enforced by (tests)

`groove::ivm::planner::tests::prepares_parameter_equality_as_param_relation_join`

## Implementation

`groove/src/ivm/planner.rs::plan_query`

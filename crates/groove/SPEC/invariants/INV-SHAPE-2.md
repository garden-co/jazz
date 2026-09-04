# INV-SHAPE-2

- Status: now
- Coverage: ✓

## Invariant

`Database::prepare_query` MUST reject queries without parameters and MUST lower only equality `column = parameter` / `parameter = column` predicates into binding joins.

## Enforced by (tests)

`groove::db::tests::prepare_query_requires_parameters_and_only_lowers_parameter_equalities`

## Implementation

groove/src/ivm/planner.rs::plan_prepared_shape; groove/src/ivm/planner.rs::Planner::lower_binding_predicate

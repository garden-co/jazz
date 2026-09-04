# INV-QUERY-17

- Status: now
- Coverage: ✓

## Invariant

SQL lowering MUST reject unsupported SELECT/set/join shapes explicitly, including `SELECT DISTINCT`, grouped/ordered/limited selects, non-inner joins, and non-`UNION ALL` set operations.

## Enforced by (tests)

`groove::ivm::planner::tests::rejects_known_but_unsupported_select_shapes`; `groove::ivm::planner::tests::rejects_non_inner_joins_and_non_union_all_sets`

## Implementation

`groove/src/ivm/planner.rs::Planner::lower_select`; `groove/src/ivm/planner.rs::Planner::lower_join`; `groove/src/ivm/planner.rs::Planner::lower_query`

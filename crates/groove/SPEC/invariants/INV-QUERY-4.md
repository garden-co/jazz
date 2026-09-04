# INV-QUERY-4

- Status: now
- Coverage: ✓

## Invariant

SQL predicate lowering MUST reject unsupported or ill-typed predicate expressions instead of lowering them approximately.

## Enforced by (tests)

`groove::ivm::planner::tests::rejects_unknown_columns_and_type_mismatches`

## Implementation

`groove/src/ivm/planner.rs::Planner::lower_comparison`

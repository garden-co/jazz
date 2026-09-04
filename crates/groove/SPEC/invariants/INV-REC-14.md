# INV-REC-14

- Status: now
- Coverage: ✓

## Invariant

SQL lowering MUST either preserve a query's semantics exactly or reject it explicitly.

## Enforced by (tests)

`groove::ivm::planner::tests::rejects_recursive_ctes_until_recursive_lowering_exists`

## Implementation

`groove/src/ivm/planner.rs::Planner::lower_query`

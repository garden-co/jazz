# INV-TICK-3

- Status: now
- Coverage: ✓

## Invariant

Commit notifications MUST contain weighted result deltas only; unchanged matching rows and base-table changes outside the query result MUST NOT be reported.

## Enforced by (tests)

groove::db::tests::subscription_reports_incremental_query_deltas_through_database_facade

## Implementation

groove/src/ivm/runtime/mod.rs::TickEvaluator::update_node

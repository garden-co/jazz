# INV-QUERY-10

- Status: now
- Coverage: ✓

## Invariant

An inner `JoinOp` MUST NOT double-count pairs where both matching sides changed in the same logical tick.

## Enforced by (tests)

`groove::db::tests::duplicate_join_subscriptions_share_state_without_double_applying_deltas`; `groove::tests::arrangement_regressions::sibling_joins_sharing_an_arrangement_do_not_double_count`

## Implementation

`groove/src/ivm/runtime/join.rs::JoinState::apply`

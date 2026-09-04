# INV-QUERY-12

- Status: now
- Coverage: ✓

## Invariant

`AntiJoin` MUST output left rows only when the total right-side multiplicity for the join key is zero.

## Enforced by (tests)

`groove::db::tests::anti_join_subscriptions_emit_left_rows_without_right_matches`; `groove::db::tests::anti_join_hydration_snapshot_filters_existing_right_matches`

## Implementation

`groove/src/ivm/runtime/join.rs::AntiJoinState::apply`

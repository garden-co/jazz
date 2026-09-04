# INV-QUERY-9

- Status: now
- Coverage: ✓

## Invariant

An inner JoinOp MUST emit joined records with weight leftweight \* rightweight for matching keys, including matches produced by changes arriving on either side.

## Enforced by (tests)

`groove::db::tests::join_subscriptions_match_left_deltas_against_maintained_right_state`; `groove::db::tests::join_subscriptions_match_right_deltas_against_maintained_left_state`

## Implementation

`groove/src/ivm/runtime/join.rs::append_join_deltas`

# INV-QUERY-14

- Status: now
- Coverage: ✓

## Invariant

Same-tick anti-join updates MUST suppress a left row that arrives with a matching right row and MUST emit a left row exactly once when it arrives in the same tick as the last blocker retracts.

## Enforced by (tests)

`groove::tests::anti_join_regressions::same_tick_left_and_blocking_right_emit_nothing`; `groove::tests::anti_join_regressions::same_tick_left_insert_and_last_blocker_retraction_emit_once`

## Implementation

`groove/src/ivm/runtime/join.rs::AntiJoinState::apply`

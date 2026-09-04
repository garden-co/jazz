# INV-TICK-10

- Status: now
- Coverage: ✓

## Invariant

Inner join output deltas MUST multiply input delta weight by stored opposite-side weight and MUST subtract one copy of the same-tick left/right cross term.

## Enforced by (tests)

groove::db::tests::query_subscriptions_support_multi_key_inner_joins

## Implementation

groove/src/ivm/runtime/join.rs::JoinState::apply; groove/src/ivm/runtime/join.rs::append_join_deltas

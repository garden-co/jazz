# INV-TICK-9

- Status: now
- Coverage: ✓

## Invariant

In accumulate mode, advancing an arrangement more than once at the same `SubTick` MUST be idempotent so shared state absorbs each tick delta only once.

## Enforced by (tests)

groove::ivm::runtime::tests::similar_join_subscriptions_share_context_independent_base_arrangements

## Implementation

groove/src/ivm/runtime/join.rs::advance_arrangement

# INV-TICK-5

- Status: now
- Coverage: ✓

## Invariant

`TickEvaluator` MUST NOT reuse node outputs across different scopes, ticks, or recursive sub-ticks; per-tick memoized outputs MUST be cleared after the tick.

## Enforced by (tests)

groove::tests::snapshot_subscription_regressions::hydrating_a_new_subscriber_must_not_steal_tick_deltas_from_existing_recursive_subscribers; groove::tests::snapshot_subscription_regressions::one_shot_queries_do_not_perturb_subscription_streams

## Implementation

groove/src/ivm/runtime/mod.rs::TickEvaluator::memo_key; groove/src/ivm/runtime/mod.rs::IvmRuntime::tick_with_params

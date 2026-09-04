# INV-OK-3

- Status: now
- Coverage: ✓

## Invariant

One-shot snapshot reads MUST NOT perturb retained subscription streams or consume future tick deltas.

## Enforced by (tests)

`groove::tests::snapshot_subscription_regressions::one_shot_queries_do_not_perturb_subscription_streams`; `groove::tests::snapshot_subscription_regressions::hydrating_a_new_subscriber_must_not_steal_tick_deltas_from_existing_recursive_subscribers`

## Implementation

`src/ivm/runtime/mod.rs::IvmRuntime::query_snapshot`; `src/ivm/runtime/mod.rs::IvmRuntime::hydration_snapshot`; `src/ivm/runtime/mod.rs::EvalContext::root_snapshot`

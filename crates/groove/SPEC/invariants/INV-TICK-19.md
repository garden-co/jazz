# INV-TICK-19

- Status: now
- Coverage: ✓

## Invariant

Hydrating or querying a graph MUST NOT perturb an existing subscription stream's future tick deltas.

## Enforced by (tests)

groove::tests::snapshot_subscription_regressions::hydrating_a_new_subscriber_must_not_steal_tick_deltas_from_existing_recursive_subscribers; groove::tests::snapshot_subscription_regressions::one_shot_queries_do_not_perturb_subscription_streams

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::hydration_snapshot; groove/src/ivm/runtime/mod.rs::GraphRuntimeView::eval_with_binding_and_table_deltas

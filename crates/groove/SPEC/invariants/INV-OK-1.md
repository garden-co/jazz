# INV-OK-1

- Status: now
- Coverage: ✓

## Invariant

For every subscription, initial snapshot plus the consolidated sum of all received deltas MUST equal a fresh one-shot recomputation of that query against current storage.

## Enforced by (tests)

`groove::db::tests::query_subscription_matches_one_shot_recompute_under_seeded_interleavings`; `groove::db::tests::graph_subscriptions_match_recompute_under_seeded_interleavings`

## Implementation

`src/ivm/runtime/mod.rs::IvmRuntime::tick_with_params`; `src/ivm/runtime/mod.rs::IvmRuntime::hydration_snapshot`; `src/ivm/runtime/recursion.rs::recompute_recursive`

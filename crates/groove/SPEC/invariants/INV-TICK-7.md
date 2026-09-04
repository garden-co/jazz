# INV-TICK-7

- Status: now
- Coverage: ✓

## Invariant

A root-scope arrangement MUST be stamped with `SubTick { tick: current_tick, sub_tick: 0 }`; only context-dependent arrangements may use the recursive evaluator's nonzero `sub_tick`.

## Enforced by (tests)

groove::db::tests::recursive_graph_subscriptions_incrementally_extend_existing_reach_with_new_edge; groove::db::tests::recursive_graph_subscriptions_incrementally_extend_new_seed_with_existing_edge

## Implementation

groove/src/ivm/runtime/mod.rs::TickEvaluator::arrangement_sub_tick

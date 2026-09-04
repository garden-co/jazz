# INV-REC-2

- Status: now
- Coverage: ✓

## Invariant

`FrontierSourceOp` MUST read only the `RecordDeltas` bound for its `FrontierName` in the current `EvalContext`; when absent it MUST yield an empty weighted record set with the declared output descriptor.

## Enforced by (tests)

`groove::db::tests::recursive_graph_subscriptions_settle_transitive_closure_in_one_tick`

## Implementation

`groove/src/ivm/runtime/mod.rs::NodeState::frontier_source`; `groove/src/ivm/runtime/recursion.rs::HydrationEvaluator::eval_node`

# INV-REC-7

- Status: now
- Coverage: ✓

## Invariant

Recursive evaluation MUST fail with `IvmRuntimeError::RecursiveIterationLimit { node, max_iters }` when the number of step iterations exceeds `RecursiveOp::max_iters`.

## Enforced by (tests)

`groove::db::tests::recursive_graphs_fail_when_frontier_exceeds_max_iters`

## Implementation

`groove/src/ivm/runtime/recursion.rs::recursive_delta`; `groove/src/ivm/runtime/recursion.rs::recompute_recursive`

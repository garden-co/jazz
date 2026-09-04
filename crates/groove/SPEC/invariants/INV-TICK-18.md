# INV-TICK-18

- Status: now
- Coverage: ✓

## Invariant

Recursive evaluation MUST stop with `RecursiveIterationLimit` when the frontier remains non-empty after `RecursiveOp.max_iters`.

## Enforced by (tests)

`groove::db::tests::recursive_graphs_fail_when_frontier_exceeds_max_iters`

## Implementation

groove/src/ivm/runtime/recursion.rs::recursive_delta; groove/src/ivm/runtime/recursion.rs::recompute_recursive

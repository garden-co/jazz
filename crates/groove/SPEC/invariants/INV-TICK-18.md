# INV-TICK-18

- Status: now
- Coverage: ✓

## Invariant

Fixpoint recursion MUST stop with `RecursiveIterationLimit` when the frontier remains non-empty after its safety `max_iters`; semantic depth bounds MUST truncate instead.

## Enforced by (tests)

`groove::db::tests::recursive_graphs_fail_when_frontier_exceeds_max_iters`; `jazz::node::tests::harness::scalar_frontier_policy_maintains_raw_evidence_without_disclosing_dependencies`

## Implementation

`groove/src/ivm/runtime/recursion.rs::recursive_delta`

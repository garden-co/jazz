# INV-TICK-17

- Status: now
- Coverage: ✓

## Invariant

Recursive recompute and incremental recursion MUST reject non-positive recursive frontier facts instead of assigning bag-recursive semantics.

## Enforced by (tests)

`groove::ivm::runtime::recursion::tests::accept_positive_rejects_raw_non_positive_frontier_deltas_before_consolidation`; `groove::ivm::runtime::recursion::tests::accept_positive_into_set_rejects_raw_non_positive_frontier_deltas_before_consolidation`

## Implementation

groove/src/ivm/runtime/recursion.rs::RecursiveState::accept_positive; groove/src/ivm/runtime/recursion.rs::accept_positive_into_set; groove/src/ivm/runtime/recursion.rs::reject_non_positive_frontier_deltas

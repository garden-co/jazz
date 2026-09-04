# INV-REC-4

- Status: now
- Coverage: ✓

## Invariant

Accepted recursive facts MUST be emitted with weight `1`; positive recursive deltas with weight greater than one MUST collapse to one accepted fact.

## Enforced by (tests)

`groove::db::tests::recursive_graph_subscriptions_collapse_duplicate_derivations`; `arrangement_regressions::recursive_incremental_ticks_do_not_inflate_shared_edge_arrangements`

## Implementation

`groove/src/ivm/runtime/recursion.rs::RecursiveState::accept_positive`; `groove/src/ivm/runtime/recursion.rs::accept_positive_into_set`

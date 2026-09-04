# INV-TICK-15

- Status: now
- Coverage: ✓

## Invariant

A recursive positive incremental tick MUST emit each newly discovered recursive fact at weight `+1` at most once and MUST collapse duplicate derivations.

## Enforced by (tests)

groove::db::tests::recursive_graph_subscriptions_collapse_duplicate_derivations; groove::tests::recursive_cycle_regressions::incremental_ticks_converge_on_cycles

## Implementation

groove/src/ivm/runtime/recursion.rs::RecursiveState::accept_positive; groove/src/ivm/runtime/recursion.rs::recursive_delta

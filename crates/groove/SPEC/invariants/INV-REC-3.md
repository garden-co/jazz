# INV-REC-3

- Status: now
- Coverage: ✓

## Invariant

Recursive facts MUST use set semantics: an encoded fact already present in `RecursiveState::accumulated` MUST NOT be emitted again or have its weight increased by duplicate derivations.

## Enforced by (tests)

`groove::db::tests::recursive_graph_subscriptions_collapse_duplicate_derivations`; `recursive_cycle_regressions::incremental_ticks_converge_on_cycles`

## Implementation

`groove/src/ivm/runtime/recursion.rs::RecursiveState::accept_positive`; `groove/src/ivm/runtime/recursion.rs::accept_positive_into_set`

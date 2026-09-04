# INV-REC-9

- Status: now
- Coverage: ✓

## Invariant

After recompute, recursive step arrangements MUST be hydrated from full table snapshots and the full accumulated weighted record set before future positive incremental use.

## Enforced by (tests)

`groove::db::tests::recursive_graph_subscriptions_incrementally_extend_existing_reach_with_new_edge`; `groove::db::tests::recursive_graph_subscriptions_incrementally_extend_new_seed_with_existing_edge`

## Implementation

`groove/src/ivm/runtime/recursion.rs::hydrate_recursive_arrangements`; `groove/src/ivm/runtime/recursion.rs::snapshot_table_deltas`; `groove/src/ivm/runtime/recursion.rs::RecursiveState::mark_step_arrangements_hydrated`

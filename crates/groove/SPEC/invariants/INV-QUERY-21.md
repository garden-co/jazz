# INV-QUERY-21

- Status: now
- Coverage: ✓

## Invariant

`ArgMaxByOp` and `ArgMinByOp` MUST emit only winner changes for touched groups, suppressing non-winner changes and net-zero group deltas.

## Enforced by (tests)

`groove::db::tests::arg_max_by_hydrates_and_tracks_winner_changes`; `groove::db::tests::arg_max_by_suppresses_non_winner_and_net_zero_deltas`; `groove::db::tests::arg_max_by_matches_naive_oracle_across_seeded_mutations`; `groove::db::tests::arg_min_by_tracks_lower_insert_and_current_winner_delete`; `groove::db::tests::arg_min_by_handles_same_tick_replacement_and_tie_by_pk_order`

## Implementation

`groove/src/ivm/runtime/mod.rs::TickEvaluator::update_arg_max_by`; `groove/src/ivm/runtime/mod.rs::TickEvaluator::update_arg_min_by`

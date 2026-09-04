# INV-READ-9

- Status: now
- Coverage: ✓

## Invariant

Global as-of reads at `GlobalTime` MUST choose per-layer winners from `jazz_global_changes` at or before the requested `global_base` and apply deletion anti-join before returning visible content.

## Enforced by (tests)

`jazz::node::tests::time_travel::query_rows_at_for_link_evaluates_read_policy_at_historical_cut`; `jazz::node::tests::time_travel::historical_read_handle_reads_exact_position_locally_when_history_complete`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::query_rows_at`; `jazz/src/node/source_resolution.rs::NodeState::projected_historical_current_rows`

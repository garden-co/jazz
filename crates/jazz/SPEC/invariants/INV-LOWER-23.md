# INV-LOWER-23

- Status: now
- Coverage: ✓

## Invariant

Position-bounded historical cuts and branch-base reads MUST use the `by_table_global_time` bounded range path when sound, returning the same rows as the full-scan currentness oracle while touching only the requested global-time range.

## Enforced by (tests)

`jazz::node::query_eval::tests::historical_cut_bounded_source_matches_full_scan_graph`; `jazz::node::query_eval::tests::historical_cut_reads_only_table_global_time_range`; `jazz::tests::branch_views::seeded_branch_view_subscription_matches_one_shot_reduction`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::bounded_global_change_records_at`; `jazz/src/node/query_eval.rs::NodeState::bounded_historical_current_rows`; `jazz/src/node/query_eval.rs::NodeState::current_rows_at`

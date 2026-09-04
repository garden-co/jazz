# INV-HIST-10

- Status: now
- Coverage: ✓

## Invariant

For `MergeStrategy::Counter`, concurrent integer deltas from their observed parent bases MUST be summed exactly.

## Enforced by (tests)

`jazz::node::tests::counter_merge::counter_merge_sums_concurrent_deltas_and_keeps_lww_columns`; `jazz::node::tests::counter_merge::counter_merge_seeded_concurrent_increments_converge_to_exact_sum`

## Implementation

`jazz/src/node/ingest.rs::merge_cells_for_heads`; `jazz/src/node/ingest.rs::counter_merge_value`

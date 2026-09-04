# INV-HIST-8

- Status: now
- Coverage: ✓

## Invariant

For `MergeStrategy::Lww`, a merged column MUST take the value from the highest made-at/`TxId` head that sets the column, and if no head sets it, from the highest made-at/`TxId` parent-union version that sets it.

## Enforced by (tests)

`jazz::node::tests::counter_merge::core_creates_merge_versions_for_concurrent_heads`; `jazz::node::tests::counter_merge::counter_merge_sums_concurrent_deltas_and_keeps_lww_columns`

## Implementation

`jazz/src/node/ingest.rs::merge_cells_for_heads`

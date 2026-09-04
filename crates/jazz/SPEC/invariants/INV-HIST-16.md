# INV-HIST-16

- Status: now
- Coverage: ✓

## Invariant

A merge value MUST be the deterministic fold over the de-duplicated raw head set, never a fold of already-merged values. Combining divergent merge versions MUST fold the union of their raw parent-closures de-duplicated by version identity (LWW argmax; `Counter` sums per-`TxId` deltas so shared ancestors count once), so divergent merges converge to the single-merger-over-the-union result.

## Enforced by (tests)

`jazz::node::tests::harness::counter_merge_of_divergent_merges_sums_raw_frontier_once`; `jazz::node::tests::harness::lww_merge_of_divergent_merges_uses_raw_argmax`; `jazz::node::tests::harness::duplicate_merges_over_same_frontier_refold_to_identical_cells`

## Implementation

`jazz/src/node/ingest.rs::raw_merge_head_tx_ids`; `jazz/src/node/ingest.rs::create_merge_version_if_needed`

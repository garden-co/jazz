# INV-EDGE-16

- Status: now
- Coverage: ✓

## Invariant

Duplicate merges of the same concurrent mergeable frontier MUST be legal (identical cells); when independent edge merges diverge, an upstream tier MUST reconcile them by folding over the de-duplicated raw head set (not by re-merging merged values), so `Counter` never double-counts a shared ancestor.

## Enforced by (tests)

`jazz::node::tests::harness::counter_merge_of_divergent_merges_sums_raw_frontier_once`; `jazz::node::tests::harness::lww_merge_of_divergent_merges_uses_raw_argmax`

## Implementation

`jazz/src/node/ingest.rs::raw_merge_head_tx_ids`; `jazz/src/node/ingest.rs::create_merge_version_if_needed`

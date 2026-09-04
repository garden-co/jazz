# INV-CONTENT-4

- Status: now
- Coverage: ✓

## Invariant

Chunk staging MUST NOT create readable application state, authorize a mutation, or publish root reachability.

## Enforced by (tests)

`jazz::node::tests::harness::failed_large_scalar_staging_publishes_no_row`; `jazz::node::tests::harness::jazz_incoming_data_rate_limit_evicts_the_rejected_root_and_publishes_no_row`; `jazz::node::tests::harness::expired_staged_tree_requires_reupload_before_row_publication`; `jazz::node::tests::harness::pushed_chunks_must_be_staged_before_the_referencing_authority_commit`; `jazz::node::tests::harness::corrupt_root_first_upload_is_rejected_without_poisoning_the_receiver`

## Implementation

`jazz/src/node/state/lifecycle.rs`; `jazz/src/node/state/commit.rs`

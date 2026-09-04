# INV-CONTENT-1

- Status: now
- Coverage: ✓

## Invariant

Jazz MUST NOT duplicate Groove's large-value tree, edit, materialization, comparison, or integrity semantics.

## Enforced by (tests)

`jazz::node::tests::harness::ordinary_oversized_scalar_write_is_staged_indirect_and_reads_logically_inline`; `jazz::node::tests::harness::synced_descriptor_reads_through_shared_opaque_chunk_backend`; `jazz::db::tests::mutations::high_level_large_value_apis_keep_descriptors_private_and_publish_edits`

## Implementation

`jazz/src/node/state/lifecycle.rs`; `jazz/src/db/mutations.rs` delegate content operations to `groove::Database`

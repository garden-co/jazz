# INV-CONTENT-5

- Status: now
- Coverage: ✓

## Invariant

Only an ordinarily authorized owner-row Insert/Update may publish a new large-value root, and every mutation path MUST use the common Groove lowering/admission boundary.

## Enforced by (tests)

`jazz::node::tests::harness::ordinary_oversized_scalar_write_is_staged_indirect_and_reads_logically_inline`; `jazz::node::tests::harness::handcrafted_large_descriptor_is_rejected_but_node_staged_preparation_can_publish`; `jazz::db::tests::mutations::high_level_large_value_apis_keep_descriptors_private_and_publish_edits`

## Implementation

`jazz/src/node/state/commit.rs::{lower_large_scalar,seal_large_value_update,seal_inherited_large_values}`

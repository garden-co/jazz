# INV-LENS-16

- Status: now
- Coverage: ✓

## Invariant

A commit unit MUST NOT be rejected or rewritten solely because its authored schema differs from the current write schema.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::old_schema_commit_units_stay_in_authored_variant_after_pointer_flip`; `jazz::node::tests::catalogue_lenses::commit_arrival_preserves_known_noncurrent_authored_variant`

## Implementation

`jazz/src/node/ingest.rs::NodeState::stage_transaction_and_versions_with_current_indexes`; `jazz/src/node/physical.rs::NodeState::version_storage_write_binding`

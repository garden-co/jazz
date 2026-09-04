# INV-LENS-10

- Status: now
- Coverage: ✓

## Invariant

New local writes MUST retain `current_write_schema.schema` as their schema discriminator and resolve storage through that schema's durable physical mapping.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::local_writes_store_versions_under_current_write_schema_storage`

## Implementation

`jazz/src/node/mod.rs::NodeState::commit_mergeable_at`, `jazz/src/node/physical.rs::NodeState::version_storage_write_binding`

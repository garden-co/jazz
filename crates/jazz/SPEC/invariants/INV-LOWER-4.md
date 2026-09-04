# INV-LOWER-4

- Status: now
- Coverage: ✓

## Invariant

Content and deletion-register versions MUST resolve through durable physical mappings to separate lineage tables; a single row MUST NOT contain both user cells and `_deletion`.

## Enforced by (tests)

`jazz::schema::tests::storage_lowering_declares_system_columns_by_shape`; `jazz::node::tests::catalogue_lenses::physical_deletion_register_spans_renamed_schemas_and_reopens`

## Implementation

`jazz/src/node/physical.rs::NodeState::version_storage_table_for_row`; `jazz/src/node/mod.rs::validate_mergeable_write_shape`

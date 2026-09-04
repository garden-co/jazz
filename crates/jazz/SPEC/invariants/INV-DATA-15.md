# INV-DATA-15

- Status: now
- Coverage: ✓

## Invariant

Each physical deletion-register lineage MUST be keyed by `(row_uuid, tx_time, tx_node_id)` and keep `_deletion` separate from content payloads.

## Enforced by (tests)

`jazz::schema::tests::storage_lowering_declares_system_columns_by_shape`; `jazz::node::tests::catalogue_lenses::physical_deletion_register_spans_renamed_schemas_and_reopens`

## Implementation

`jazz/src/node/physical.rs::physical_version_storage_tables`

# INV-DATA-14

- Status: now
- Coverage: ✓

## Invariant

Each physical content-history lineage MUST be keyed by `(row_uuid, tx_time, tx_node_id)` and store schema-versioned user payloads.

## Enforced by (tests)

`jazz::schema::tests::logical_history_descriptor_has_composite_primary_key`; `jazz::node::tests::catalogue_lenses::publishing_schema_registers_new_physical_tables_live`

## Implementation

`jazz/src/node/physical.rs::physical_version_storage_tables`

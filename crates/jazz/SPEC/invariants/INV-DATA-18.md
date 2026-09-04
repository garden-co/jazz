# INV-DATA-18

- Status: now
- Coverage: ✓

## Invariant

Per-lineage global-current tables MUST be keyed by `row_uuid`; versioned content projections MUST expose all logical user columns and physical indexes only for declared columns.

## Enforced by (tests)

`jazz::schema::tests::storage_lowering_declares_system_columns_by_shape`; `jazz::node::tests::queries::physical_index_backfills_existing_rows_and_read_cost_ignores_schema_variant_count`

## Implementation

`jazz/src/node/physical.rs::physical_version_storage_tables`; `jazz/src/node/physical.rs::NodeState::register_physical_current_variant_projections`

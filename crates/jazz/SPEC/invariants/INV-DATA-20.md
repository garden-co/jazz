# INV-DATA-20

- Status: now
- Coverage: ✓

## Invariant

`JazzSchema::lower_to_groove()` MUST provide fixed system storage; full node open MUST add all application tables derived from durable physical mappings and direct record stores.

## Enforced by (tests)

`jazz::schema::tests::storage_lowering_declares_system_columns_by_shape`; `jazz::node::tests::catalogue_lenses::durable_catalogue_values_pointer_and_physical_mappings_survive_restart`

## Implementation

`jazz/src/schema.rs::JazzSchema::lower_to_groove`; `jazz/src/node/mod.rs::NodeState::open_full_database`; `jazz/src/node/physical.rs::physical_version_storage_tables`

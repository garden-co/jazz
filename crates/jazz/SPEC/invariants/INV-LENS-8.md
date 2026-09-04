# INV-LENS-8

- Status: now
- Coverage: ✓

## Invariant

Durable catalogue schemas, lenses, current-write pointer, schema-version aliases, and physical mappings MUST survive node restart. Installing an authority snapshot MUST preserve the node-local storage identity of an already-open schema so pre-snapshot local writes remain addressable.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::durable_catalogue_values_pointer_and_physical_mappings_survive_restart`; `jazz::node::tests::catalogue_lenses::catalogue_snapshot_preserves_active_schema_storage_identity`

## Implementation

`jazz/src/node/mod.rs::NodeState::open_catalogue_stage`; `jazz/src/node/ingest.rs::NodeState::plan_trusted_catalogue_snapshot`; `jazz/src/node/mod.rs::NodeState::write_schema_version_mapping_to_batch`

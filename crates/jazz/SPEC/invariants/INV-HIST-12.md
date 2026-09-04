# INV-HIST-12

- Status: now
- Coverage: ✓

## Invariant

Accepted globally settled versions that become per-layer winners MUST be reflected in the physical lineage's content or register global-current table.

## Enforced by (tests)

`jazz::node::tests::sync::accepted_fates_maintain_global_current_tables`; `jazz::node::tests::catalogue_lenses::physical_deletion_register_spans_renamed_schemas_and_reopens`

## Implementation

`jazz/src/node/ingest.rs::write_global_current_update`; `jazz/src/node/physical.rs::NodeState::physical_current_table_for_schema`

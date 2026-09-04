# INV-LENS-9

- Status: now
- Coverage: ✓

## Invariant

Publishing a non-genesis schema and its lineage-defining lens MUST durably stage the complete ordered bundle, keep it invisible while every physical table and schema variant is registered, then durably activate it before acknowledging or draining parked work. One activation batch replaces its pending obligation with the schema, lens, physical mapping, and active receipt; reopen resumes staged activation idempotently, and reconnect retry does not duplicate a parked unit's projection.

## Enforced by (tests)

`jazz::node::tests::harness::{non_genesis_schema_activates_only_with_its_ordered_lineage_bundle,catalogue_arrival_drains_schema_orphan_commit_units}`

## Implementation

`jazz/src/node/{ingest/catalogue.rs,state/catalogue.rs}::NodeState::{apply_publish_schema_with_lens,write_active_schema_lineage_to_batch}`

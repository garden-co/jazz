# INV-LENS-9

- Status: next
- Coverage: untested

## Invariant

Publishing a non-genesis schema and its lineage-defining lens MUST durably stage the complete ordered bundle, keep it invisible while every physical table and schema variant is registered, then durably activate it before acknowledging or draining parked work; reopen MUST resume staged activation idempotently.

## Enforced by (tests)

NONE-FOUND

## Implementation

`jazz/src/node/ingest.rs::NodeState::apply_publish_schema_with_lens`, `jazz/src/node/physical.rs::NodeState::synchronize_physical_version_tables`

# INV-LENS-5

- Status: now
- Coverage: ✓

## Invariant

Unknown-schema commit units MUST park without ingesting a transaction and MUST drain when the corresponding `SchemaVersion` catalogue value arrives.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::catalogue_arrival_drains_schema_orphan_commit_units`

## Implementation

`jazz/src/node/ingest.rs::NodeState::apply_publish_schema`, `jazz/src/node/ingest.rs::NodeState::ingest_commit_unit`

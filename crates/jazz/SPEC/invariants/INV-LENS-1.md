# INV-LENS-1

- Status: now
- Coverage: ✓

## Invariant

A published `SchemaVersion` MUST have `schema.id == schema.schema.version_id()`; every non-genesis schema MUST be admitted in one catalogue operation with its lineage-defining lens before it is known or writeable.

## Enforced by (tests)

`jazz::node::tests::harness::non_genesis_schema_activates_only_with_its_ordered_lineage_bundle`; `jazz::node::tests::harness::schema_lineage_gaps_and_inactive_sources_park_durably_then_drain_in_order`

## Implementation

`jazz/src/node/ingest.rs::NodeState::apply_publish_schema_with_lens`; `jazz/src/tools/server/runtime_catalogue.rs::publish_runtime_catalogue`

# INV-LOWER-3

- Status: now
- Coverage: ✓

## Invariant

Node-local aliases in `jazz_nodes.id` and `jazz_schema_versions.id` MUST NOT be wire identities; wire tx/schema references MUST use `NodeUuid` and `SchemaVersionId`.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::wire_commit_units_preserve_node_and_schema_uuids_not_local_aliases`

## Implementation

`jazz/src/node/currency.rs::NodeState::decode_history_record`; `jazz/src/node/codec.rs::version_tx_id_from_aliases`

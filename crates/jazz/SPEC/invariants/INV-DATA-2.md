# INV-DATA-2

- Status: now
- Coverage: ✓

## Invariant

`NodeAlias` and `SchemaVersionAlias` MUST be node-local storage aliases allocated in `jazz_nodes` and `jazz_schema_versions`; all egress from stored rows MUST resolve aliases back to `NodeUuid` and `SchemaVersionId`. Missing, conflicting, or out-of-range durable mappings fail closed before decode or mutation.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::schema_version_id_round_trips_through_wire_ingest_and_recovery`; `jazz::node::tests::harness::failed_node_alias_persistence_leaves_no_resident_alias_or_dependent_history_for_reopen`; `jazz::node::tests::harness::reopening_rejects_a_schema_version_with_two_durable_aliases`; `jazz::node::tests::harness::reopening_rejects_schema_alias_that_cannot_lower_to_a_groove_variant_tag`

## Implementation

`node/mod.rs::ensure_node_alias`; `node/mod.rs::ensure_schema_version_alias`; `node/mod.rs::version_tx_id`; `node/mod.rs::version_record_from_row`; `node/recovery.rs::recover_from_storage`; `node/state/lifecycle.rs::open_catalogue`

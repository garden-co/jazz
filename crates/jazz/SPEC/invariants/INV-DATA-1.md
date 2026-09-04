# INV-DATA-1

- Status: now
- Coverage: ✓

## Invariant

Stable wire identity fields MUST use the UUID newtypes (`NodeUuid`, `RowUuid`, `SchemaVersionId`, `MigrationLensId`, `BranchId`, `AuthorSubject`) in wire byte order; node-local alias types MUST NOT be part of wire identity.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::schema_version_id_round_trips_through_wire_ingest_and_recovery`; `jazz::node::tests::sync::wire_record_round_trips_through_history_bytes`

## Implementation

`ids.rs::{NodeUuid,RowUuid,SchemaVersionId,MigrationLensId,BranchId,AuthorSubject}::{from_bytes,to_bytes,as_bytes}`; `node/mod.rs::{ensure_node_alias,ensure_schema_version_alias,version_tx_id,version_record_from_row}`

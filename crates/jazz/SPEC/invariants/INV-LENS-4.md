# INV-LENS-4

- Status: now
- Coverage: ✓

## Invariant

Every stored content/register history row MUST carry a schema-version alias, and every wire `VersionRecord` MUST expose the full `SchemaVersionId`.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::schema_version_id_round_trips_through_wire_ingest_and_recovery`

## Implementation

`jazz/src/node/codec.rs::HistoryRowRecord`, `jazz/src/node/codec.rs::RegisterRowRecord`, `jazz/src/protocol.rs::VersionRecord`

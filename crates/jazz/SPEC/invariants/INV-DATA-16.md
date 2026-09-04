# INV-DATA-16

- Status: now
- Coverage: ✓

## Invariant

The wire row descriptor for replicated row payloads MUST include only `row_uuid`, `parents`, nullable `_deletion`, and nullable `user_{col}` cells; receiver-local currentness and authority-state columns MUST be excluded.

## Enforced by (tests)

`jazz::node::tests::sync::wire_record_round_trips_through_history_bytes`

## Implementation

`schema.rs::TableSchema::wire_record_descriptor`; `protocol.rs::VersionRecord::encode`; `protocol.rs::VersionRecord::{row_uuid,parents,deletion,cell_at}`

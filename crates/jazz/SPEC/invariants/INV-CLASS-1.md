# INV-CLASS-1

- Status: now
- Coverage: ✓

## Invariant

Column-class shipping principle: upstream-decided mutable state and node-local derived state MUST NOT be shipped as replicated row payload.

## Enforced by (tests)

`jazz::node::tests::sync::wire_record_round_trips_through_history_bytes`

## Implementation

`schema.rs::TableSchema::wire_record_descriptor`; `protocol.rs::VersionRecord::encode`; `protocol.rs::VersionRecord::{row_uuid,parents,deletion,cell_at}`

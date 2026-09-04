# INV-SYNC-21

- Status: now
- Coverage: ✓

## Invariant

Wire `TxId` and row-version payloads MUST use node UUIDs and schema version IDs, not node-local integer aliases.

## Enforced by (tests)

`jazz::node::tests::sync::wire_record_round_trips_through_history_bytes`

## Implementation

`protocol.rs::tx_id_value`; `protocol.rs::tx_id_from_value`; `protocol.rs::VersionRecord`; `node/codec.rs::transaction_values`

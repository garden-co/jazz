# INV-EDGE-11

- Status: now
- Coverage: untested

## Invariant

Fate and durability MUST remain separate axes: edge-accepted does not imply `DurabilityTier::Global`; receivers MUST raise observed durability only from explicit durability claims.

## Enforced by (tests)

NONE-FOUND

## Implementation

`protocol.rs::SyncMessage::FateUpdate`, `node/ingest.rs::NodeState::apply_sync_message`, `tx.rs::DurabilityTier`

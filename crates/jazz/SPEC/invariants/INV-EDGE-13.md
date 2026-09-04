# INV-EDGE-13

- Status: now
- Coverage: ✓

## Invariant

Resubmitting the same commit unit through another edge MUST be idempotent by `TxId` when the payload matches, and conflicting payloads with the same `TxId` MUST be rejected as `ConflictingCommitUnit`.

## Enforced by (tests)

`jazz::tests::four_tier::four_tier_topology_relays_pending_units_and_core_fates`

## Implementation

`node/ingest.rs::NodeState::ingest_relay_commit_unit_once`, `node/ingest.rs::NodeState::ingest_commit_unit_once`

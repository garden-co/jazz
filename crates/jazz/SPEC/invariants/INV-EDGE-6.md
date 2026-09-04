# INV-EDGE-6

- Status: now
- Coverage: ✓

## Invariant

`TxKind::Exclusive` acceptance MUST be decided by core, the serialization point; edge authority MUST NOT make exclusive acceptance final.

## Enforced by (tests)

`jazz::tests::four_tier::four_tier_topology_relays_pending_units_and_core_fates`

## Implementation

`node/ingest.rs::NodeState::validate_exclusive_commit_unit`, `node/ingest.rs::NodeState::ingest_commit_unit_once`

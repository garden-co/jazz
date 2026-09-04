# INV-EDGE-2

- Status: now
- Coverage: ✓

## Invariant

A relay MUST store/forward `TxKind::Mergeable` and `TxKind::Exclusive` commit units as `Fate::Pending` with `DurabilityTier::Local` and MUST NOT assign an authority fate.

## Enforced by (tests)

`jazz::tests::four_tier::four_tier_topology_relays_pending_units_and_core_fates`

## Implementation

`node/ingest.rs::NodeState::ingest_relay_commit_unit_once`

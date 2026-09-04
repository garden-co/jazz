# INV-SYNC-5

- Status: now
- Coverage: ✓

## Invariant

A receiver applying a fate update MUST NOT move `global_time` backward and MUST raise observed durability only by a supplied `Some(DurabilityTier)` claim using monotone max semantics; `None` MUST leave durability unchanged.

## Enforced by (tests)

`jazz::node::tests::sync::commit_units_sync_upstream_and_fates_flow_back`; `jazz::tests::four_tier::four_tier_topology_relays_pending_units_and_core_fates`

## Implementation

`node/ingest.rs::apply_fate_update`; `node/ingest.rs::fate_update_durability_claim`

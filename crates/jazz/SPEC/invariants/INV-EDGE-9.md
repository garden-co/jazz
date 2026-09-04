# INV-EDGE-9

- Status: now
- Coverage: ✓

## Invariant

A cancelled or missing permission scope MUST NOT satisfy the edge permission gate; after restart, deferred edge-fate gates and retained scope refs are absent until client outbox redelivery reopens the gate, while already edge-accepted units MUST survive from edge storage without redelivery.

## Enforced by (tests)

`jazz::tests::four_tier::edge_restart_recovers_deferred_fate_from_client_outbox_redelivery`; `jazz::tests::four_tier::edge_restart_preserves_edge_accepted_unit_without_redelivery`

## Implementation

`peer.rs::PeerState::ingest_edge_mergeable_commit_unit`; `peer.rs::PeerState::drain_deferred_edge_fates`; `peer.rs::PeerState::permission_scopes_settled_for`; `node/ingest.rs::NodeState::ingest_relay_commit_unit_once`; `node/ingest.rs::NodeState::apply_fate_update`

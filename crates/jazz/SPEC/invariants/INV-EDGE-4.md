# INV-EDGE-4

- Status: now
- Coverage: ✓

## Invariant

An edge MUST NOT assign a mergeable fate until the needed permission-scope subscription has delivered an initial settled result; before that, the transaction MUST remain pending and deferred.

## Enforced by (tests)

`jazz::tests::four_tier::edge_defers_mergeable_fate_until_permission_scope_settles`

## Implementation

`peer.rs::PeerState::ingest_edge_mergeable_commit_unit`, `peer.rs::PeerState::permission_scopes_settled_for`, `peer.rs::PeerState::drain_deferred_edge_fates`

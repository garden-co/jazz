# INV-EDGE-3

- Status: now
- Coverage: ✓

## Invariant

An edge-client link MUST terminate exactly one client author identity as `PeerRole::ClientLink { identity }`, and downstream reads on that link MUST use that identity for policy composition.

## Enforced by (tests)

`jazz::tests::four_tier::edge_peer_terminates_client_identity_and_relays_upstream`

## Implementation

`peer.rs::PeerState::edge_client`, `peer.rs::PeerState::identity`, `peer.rs::PeerState::current_rows_update`

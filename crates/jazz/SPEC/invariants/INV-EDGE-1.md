# INV-EDGE-1

- Status: now
- Coverage: ✓

## Invariant

A `PeerRole::Relay` link MUST use `AuthorSubject::SYSTEM` as its link identity and MUST NOT terminate a client identity.

## Enforced by (tests)

`jazz::tests::four_tier::edge_peer_terminates_client_identity_and_relays_upstream`

## Implementation

`peer.rs::PeerRole::identity`

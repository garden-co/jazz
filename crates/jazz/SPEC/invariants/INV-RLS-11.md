# INV-RLS-11

- Status: now
- Coverage: ✓

## Invariant

Relay peer links MUST use `AuthorSubject::SYSTEM`; edge-client peer links MUST use the terminated client `AuthorSubject` for policy-composed reads.

## Enforced by (tests)

`jazz::node::tests::policies_rls::relay_and_edge_peer_identities_drive_policy_composed_reads`

## Implementation

jazz/src/peer.rs::PeerRole::identity

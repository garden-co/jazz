# INV-API-21

- Status: now
- Coverage: ✓

## Invariant

A subscriber `PeerConnection::tick` MUST serve subscriptions under the `AuthorSubject` passed to `Node::accept_subscriber`, not under the serving node's own identity.

## Enforced by (tests)

`jazz::db::tests::accepted_subscriber_is_served_under_subscriber_author_identity`

## Implementation

`jazz/src/db.rs::Node::accept_subscriber`; `jazz/src/db.rs::PeerConnection::tick`; `jazz/src/peer.rs::PeerState::client_link`

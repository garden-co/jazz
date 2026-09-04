# INV-SYNC-23

- Status: now
- Coverage: untested

## Invariant

A serving peer MUST reject a capability-gapped live subscription with `SyncMessage::SubscribeRejected` addressed to the requested `SubscriptionKey`; the rejected subscription MUST NOT become active, `Unsubscribe` for it is a no-op, and the connection MUST keep serving other subscriptions.

## Enforced by (tests)

NONE-FOUND

## Implementation

`protocol.rs::SyncMessage::SubscribeRejected`; `db.rs::PeerConnection::tick`; `peer.rs::PeerState::rehydrate_query_for_subscription_with_opts`

# INV-API-18

- Status: now
- Coverage: ✓

## Invariant

`Db::subscribe` MUST announce newly registered subscriptions to all existing upstream connections so query-driven sync can request remote completion on the next tick.

## Enforced by (tests)

`jazz::db::tests::db_sync_surface_round_trips_subscription_to_client`

## Implementation

`jazz/src/db.rs::Db::subscribe`; `jazz/src/db.rs::PeerConnection::announce_subscription`; `jazz/src/db.rs::PeerConnection::tick`

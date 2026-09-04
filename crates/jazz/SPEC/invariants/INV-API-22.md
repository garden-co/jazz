# INV-API-22

- Status: now
- Coverage: ✓

## Invariant

`Db::tick()` MUST service every registered `PeerConnection` exactly once by calling `PeerConnection::tick`.

## Enforced by (tests)

`jazz::db::tests::db_sync_surface_round_trips_subscription_to_client`; `jazz::db::tests::db_sync_surface_uploads_client_writes_for_authority_fate`

## Implementation

`jazz/src/db.rs::Db::tick`

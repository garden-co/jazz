# INV-API-16

- Status: now
- Coverage: ✓

## Invariant

`Transport` implementations MUST be non-blocking; `try_recv() == None` MUST mean no inbound message is currently staged and MUST NOT be interpreted by `Db` as disconnect.

## Enforced by (tests)

`jazz::db::tests::db_sync_surface_round_trips_subscription_to_client`; `jazz::db::tests::db_sync_surface_uploads_client_writes_for_authority_fate`

## Implementation

`jazz/src/db.rs::Transport`; `jazz/src/db.rs::PeerConnection::tick`

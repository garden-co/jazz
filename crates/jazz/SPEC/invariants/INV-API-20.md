# INV-API-20

- Status: now
- Coverage: ✓

## Invariant

An upstream `PeerConnection::tick` MUST upload each locally-authored `TxId` at most once per connection by reading `commit_unit_for(tx_id)`, sending it, and marking it uploaded.

## Enforced by (tests)

`jazz::db::tests::db_sync_surface_uploads_client_writes_for_authority_fate`

## Implementation

`jazz/src/db.rs::PeerConnection::tick`

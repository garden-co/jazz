# INV-API-13

- Status: now
- Coverage: ✓

## Invariant

Every local write method MUST return a `WriteHandle` carrying the affected `RowUuid`, backing `TxId`, and local durability tier.

## Enforced by (tests)

`jazz::db::tests::db_facade_opens_writes_and_reads_todos_end_to_end`; `jazz::db::tests::db_sync_surface_uploads_client_writes_for_authority_fate`

## Implementation

`jazz/src/db.rs::Db::write_mergeable`; `jazz/src/db.rs::WriteHandle`

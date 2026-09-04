# INV-API-12

- Status: now
- Coverage: untested

## Invariant

`Db::restore` MUST reject empty cell data with `ErrorCode::Schema` and MUST lower a non-empty restore to content write plus `DeletionEvent::Restored`.

## Enforced by (tests)

restore success covered by `jazz::db::tests::db_facade_mutation_lifecycle_writes_reads_deletes_and_restores`; empty-data rejection NONE-FOUND

## Implementation

`jazz/src/db.rs::Db::restore`; `jazz/src/db.rs::Db::write_mergeable`

# INV-API-11

- Status: now
- Coverage: ✓

## Invariant

`Db::delete` MUST lower to a mergeable commit with `DeletionEvent::Deleted` and make the row absent from current reads after local application.

## Enforced by (tests)

`jazz::db::tests::db_facade_mutation_lifecycle_writes_reads_deletes_and_restores`; `jazz::node::tests::queries::db_facade_current_rows_match_seeded_create_delete_sequence`

## Implementation

`jazz/src/db.rs::Db::delete`; `jazz/src/db.rs::Db::write_mergeable`

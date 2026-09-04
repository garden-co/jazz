# INV-API-9

- Status: now
- Coverage: ✓

## Invariant

`Db::update` MUST preserve omitted fields for a locally present row by merging the patch over the row's current local cells.

## Enforced by (tests)

`jazz::db::tests::db_facade_mutation_lifecycle_writes_reads_deletes_and_restores`

## Implementation

`jazz/src/db.rs::Db::update`; `jazz/src/db.rs::Db::merge_existing_cells`

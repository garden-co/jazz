# INV-API-10

- Status: now
- Coverage: ✓

## Invariant

`Db::upsert` MUST merge supplied cells over current cells when the row exists locally and MUST write supplied cells directly when the row does not exist locally.

## Enforced by (tests)

`jazz::db::tests::upsert_merges_existing_rows_but_writes_absent_rows_directly`

## Implementation

`jazz/src/db.rs::Db::upsert`; `jazz/src/db.rs::Db::local_row`; `jazz/src/db.rs::Db::merge_existing_cells`

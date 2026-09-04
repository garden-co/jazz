# INV-API-8

- Status: now
- Coverage: ✓

## Invariant

`Db::insert` MUST generate the row id using its configured `RowIdSource`; `Db::insert_with_id` MUST use the caller-supplied `RowUuid`.

## Enforced by (tests)

`jazz::db::tests::db_facade_opens_writes_and_reads_todos_end_to_end`; `jazz::db::tests::db_query_builder_expresses_s1_shaped_filters_and_include_modes`

## Implementation

`jazz/src/db.rs::Db::insert`; `jazz/src/db.rs::Db::insert_with_id`; `jazz/src/db.rs::RowIdSource`; `jazz/src/db.rs::SeededRowIdSource::next_row_id`; `jazz/src/db.rs::ProductionRowIdSource::next_row_id`

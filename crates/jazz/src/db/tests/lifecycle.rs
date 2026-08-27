//! Database opening, closing, and facade-level lifecycle tests.

use super::*;
use groove::storage::MemoryStorage;

#[test]
fn only_production_row_ids_enable_the_fresh_insert_proof() {
    let seeded = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    assert!(!seeded.row_id_source_guarantees_fresh);

    let schema = doctest_support::schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let production = block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x12; 16]),
            author: AuthorSubject::for_test_bytes([0xa2; 16]),
        },
    )))
    .unwrap();
    assert!(production.row_id_source_guarantees_fresh);
}

#[test]
fn db_facade_opens_writes_and_reads_todos_end_to_end() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("learn the db facade", false),
            Default::default(),
        )
        .unwrap();
    let todo = write.row_uuid();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let read_rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&read_rows), vec![todo]);
    assert_eq!(
        read_rows[0].cell(table, "title"),
        Some(Value::String("learn the db facade".to_owned()))
    );
    assert_eq!(read_rows[0].cell(table, "done"), Some(Value::Bool(false)));

    let one_row = prepared_one(&db, &query).unwrap();
    assert_eq!(one_row.row_uuid(), todo);
    assert_eq!(
        one_row.cell(table, "title"),
        Some(Value::String("learn the db facade".to_owned()))
    );

    let all_rows = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(row_ids(&all_rows), vec![todo]);
    assert_eq!(all_rows[0].cell(table, "done"), Some(Value::Bool(false)));
}

#[test]
fn db_close_is_idempotent() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    db.insert(
        "todos",
        doctest_support::todo_cells("close me", false),
        Default::default(),
    )
    .unwrap();

    doctest_support::block_on(db.close()).unwrap();
    doctest_support::block_on(db.close()).unwrap();
}

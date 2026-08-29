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
        MemoryStorage::new(&refs).expect("valid memory storage families"),
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

#[test]
fn foreground_handoff_high_water_includes_an_unsubmitted_public_write() {
    // This lifecycle-only receipt deliberately reaches the hidden handoff
    // boundary: applications cannot query or seed a runtime HLC. The write is
    // otherwise public and has no upstream, so it proves a clean lease return
    // covers a TxId that was minted locally but never submitted.
    let first = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let floor = TxTime::new(1_000_000, 17);
    doctest_support::block_on(first.seed_foreground_tx_time_high_water(floor));

    let first_write = first
        .insert(
            "todos",
            doctest_support::todo_cells("unsubmitted foreground write", false),
            Default::default(),
        )
        .unwrap();
    let first_tx = first_write.mergeable_tx_id();
    assert!(first_tx.time > floor);
    assert_eq!(
        doctest_support::block_on(first.foreground_tx_time_high_water()),
        first_tx.time
    );

    // A later owner of the same node begins strictly above every identity the
    // prior runtime allocated, even when its wall clock has not advanced.
    let second = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    doctest_support::block_on(second.seed_foreground_tx_time_high_water(first_tx.time));
    let second_write = second
        .insert(
            "todos",
            doctest_support::todo_cells("continued foreground write", false),
            Default::default(),
        )
        .unwrap();
    assert!(second_write.mergeable_tx_id().time > first_tx.time);
}

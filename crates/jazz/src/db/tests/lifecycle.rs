//! Database opening, closing, and facade-level lifecycle tests.

use super::*;

#[test]
fn db_facade_opens_writes_and_reads_todos_end_to_end() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = doctest_support::block_on(db.insert(
        "todos",
        doctest_support::todo_cells("learn the db facade", false),
    ))
    .unwrap();
    let todo = write.row_uuid();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let prepared = db.prepare_query(&query).unwrap();
    let read_rows = doctest_support::block_on(db.read(&prepared)).unwrap();
    assert_eq!(row_ids(&read_rows), vec![todo]);
    assert_eq!(
        read_rows[0].cell(table, "title"),
        Some(Value::String("learn the db facade".to_owned()))
    );
    assert_eq!(read_rows[0].cell(table, "done"), Some(Value::Bool(false)));

    let one_row = doctest_support::block_on(db.one(&prepared))
        .unwrap()
        .unwrap();
    assert_eq!(one_row.row_uuid(), todo);
    assert_eq!(
        one_row.cell(table, "title"),
        Some(Value::String("learn the db facade".to_owned()))
    );

    let all_rows = doctest_support::block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&all_rows), vec![todo]);
    assert_eq!(all_rows[0].cell(table, "done"), Some(Value::Bool(false)));
}

#[test]
fn db_close_flushes_the_unique_owner() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    doctest_support::block_on(db.insert("todos", doctest_support::todo_cells("close me", false)))
        .unwrap();
    doctest_support::block_on(db.close()).unwrap();
}

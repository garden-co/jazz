//! A read that leaves rows out because the device cannot open them says
//! which (`E2EE_PROTOCOL.md` §4.3): the bindings' one-shot read answers its
//! rows with the rows it left out, so a reader can tell the result, or a
//! page of it, is incomplete.

mod common;

use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::{Db, SerializedReadResult, UnreadableRow, block_on};
use jazz::ids::RowUuid;
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee_client::MemoryStore;

/// The bindings' read of `query`, local only: its rows' ids and what it
/// left out.
fn read(db: &Db, query: &Query) -> (Vec<RowUuid>, Vec<UnreadableRow>) {
    let bytes = postcard::to_allocvec(query).unwrap();
    let (result, unreadable) =
        block_on(db.all_serialized_query_reporting(&bytes, local(), None, None, None)).unwrap();
    let SerializedReadResult::Rows(rows) = result else {
        panic!("a relation snapshot");
    };
    (rows.iter().map(|row| row.row_uuid()).collect(), unreadable)
}

/// A note titled `title`, as a writer without the hooks stores it: its
/// encrypted cells are plain bytes no key opens.
fn forged(space: uuid::Uuid, title: &str) -> BTreeMap<String, Value> {
    let mut cells = note(space, "ignored");
    for column in ["body", "note", "attachment", "rank"] {
        cells.insert(column.to_owned(), Value::Bytes(b"not sealed".to_vec()));
    }
    cells.insert("title".to_owned(), text(title));
    cells
}

/// Contract: alice's read answers the rows it could open and names each row
/// it left out, once, with the protocol's code. A page counts the rows it
/// left out toward its limit (the window applies before rows are opened),
/// so every page that came back short says so, and paging on by the limit
/// visits every row exactly once.
///
/// ```text
/// alice ──sealed a, c, e──► server ◄──forged b, d── mallory
///                             │
/// alice reads `title` order, 2 per page:
///   [a] + b left out │ [c] + d left out │ [e]
/// ```
#[test]
fn a_read_names_the_rows_it_left_out_and_a_page_counts_them() {
    let (server, _client, db, _link, space) = client_in_space();
    let mut ids = BTreeMap::new();
    for title in ["a", "c", "e"] {
        let mut cells = note(space, SECRET);
        cells.insert("title".to_owned(), text(title));
        let row = block_on(db.insert("notes", cells, Default::default())).unwrap();
        ids.insert(title, row.row_uuid());
    }
    pump(&server, &db);
    for title in ["b", "d"] {
        let row = block_on(server.insert("notes", forged(space, title), Default::default()))
            .unwrap()
            .row_uuid();
        ids.insert(title, row);
    }
    let query = Query::from("notes").order_by("title", OrderDirection::Asc);
    let _live =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);

    let (rows, unreadable) = read(&db, &query);
    assert_eq!(rows, [ids["a"], ids["c"], ids["e"]]);
    assert_eq!(
        unreadable.iter().map(|row| row.id).collect::<Vec<_>>(),
        [ids["b"], ids["d"]],
        "each row left out is named once, though all its encrypted cells failed"
    );
    assert!(
        unreadable
            .iter()
            .all(|row| row.table == "notes" && row.code == "invalid-ciphertext"),
        "{unreadable:?}"
    );

    let mut seen = Vec::new();
    let pages = [
        (vec![ids["a"]], vec![ids["b"]]),
        (vec![ids["c"]], vec![ids["d"]]),
        (vec![ids["e"]], vec![]),
    ];
    for (page, (rows, left_out)) in pages.into_iter().enumerate() {
        let (got, unreadable) = read(&db, &query.clone().limit(2).offset(page * 2));
        assert_eq!(got, rows, "page {page}");
        let missing: Vec<_> = unreadable.iter().map(|row| row.id).collect();
        assert_eq!(missing, left_out, "page {page}");
        seen.extend(got);
        seen.extend(missing);
    }
    seen.sort();
    let mut every: Vec<_> = ids.values().copied().collect();
    every.sort();
    assert_eq!(seen, every, "the pages cover every row once");

    // A read of plain columns only opens nothing and leaves nothing out.
    let (rows, unreadable) = read(&db, &query.clone().select(["title"]));
    assert_eq!(rows.len(), 5);
    assert!(unreadable.is_empty());
}

/// Contract: a row sealed under a version of its space's key that alice
/// does not hold is left out of her read as `key-pending` (the runtime
/// fetches the key), and named in the read itself, not only in the
/// runtime's later report.
///
/// ```text
/// writer ──sealed under v1──► server ──► alice (holds no key): left out, key-pending
/// ```
#[test]
fn a_row_under_a_key_not_held_is_named_key_pending() {
    let server = authority();
    let writer = open_client(Rc::new(MemoryStore::new()));
    let (writer_db, _w, _) = online(&writer, &server, None);
    let space = project(&writer_db, "plans");
    hold_new_space(&writer, &writer_db, space);
    let row = block_on(writer_db.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    pump(&server, &writer_db);

    let alice = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&alice, &server, None);
    let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
    let _live =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);

    let (rows, unreadable) = read(&db, &query);
    assert!(rows.is_empty());
    assert_eq!(unreadable.len(), 1, "{unreadable:?}");
    assert_eq!(unreadable[0].id, row);
    assert_eq!(unreadable[0].code, "key-pending");
}

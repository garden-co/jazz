//! A subscription that leaves rows out because the device cannot open them
//! says which (`E2EE_PROTOCOL.md` §4.3), as a one-shot read does
//! (`unreadable_reads.rs`): its opening names them, and a later delta names
//! them again whenever the list changed (a row became unreadable, readable,
//! or left the result), so a reader always knows its result is incomplete.

mod common;

use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::{Db, SubscriptionEvent, SubscriptionStream, UnreadableRow, block_on};
use jazz::ids::RowUuid;
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee_client::MemoryStore;

/// What a reader holds after applying the events it was told so far.
#[derive(Default)]
struct Told {
    /// The result's row ids, in order.
    rows: Vec<RowUuid>,
    /// The rows the result leaves out, as last told.
    unreadable: Vec<UnreadableRow>,
    /// Deltas that changed no row but told a new list.
    list_only: usize,
}

impl Told {
    /// Apply every event `stream` has now.
    fn take(&mut self, stream: &mut SubscriptionStream) {
        while let Some(event) = stream.try_next_event() {
            let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                unreadable,
                ..
            } = event
            else {
                panic!("{event:?}");
            };
            if reset {
                self.rows.clear();
                assert!(unreadable.is_some(), "an opening restates the list");
            }
            for row in &removed {
                self.rows.retain(|id| *id != row.row_uuid);
            }
            for row in &updated {
                self.rows.retain(|id| *id != row.row.row_uuid());
            }
            let mut placed: Vec<_> = added.iter().chain(&updated).collect();
            placed.sort_by_key(|row| row.index);
            for row in placed {
                self.rows.insert(row.index, row.row.row_uuid());
            }
            if !reset && added.is_empty() && updated.is_empty() && removed.is_empty() {
                self.list_only += usize::from(unreadable.is_some());
            }
            if let Some(unreadable) = unreadable {
                self.unreadable = unreadable.to_vec();
            }
        }
    }

    fn unreadable_ids(&self) -> Vec<RowUuid> {
        self.unreadable.iter().map(|row| row.id).collect()
    }
}

/// A note titled `title` in `space`, as alice seals it.
fn sealed(db: &Db, space: uuid::Uuid, title: &str) -> RowUuid {
    let mut cells = note(space, SECRET);
    cells.insert("title".to_owned(), text(title));
    block_on(db.insert("notes", cells, Default::default()))
        .unwrap()
        .row_uuid()
}

/// A note titled `title`, written straight to the server by a writer
/// without the hooks: its encrypted cells are plain bytes no key opens.
fn forged(server: &Db, space: uuid::Uuid, title: &str) -> RowUuid {
    let mut cells: BTreeMap<String, Value> = note(space, "ignored");
    for column in ["body", "note", "attachment", "rank"] {
        cells.insert(column.to_owned(), Value::Bytes(b"not sealed".to_vec()));
    }
    cells.insert("title".to_owned(), text(title));
    block_on(server.insert("notes", cells, Default::default()))
        .unwrap()
        .row_uuid()
}

/// Contract: alice's subscription names the rows it left out, once each,
/// with the protocol's code, and keeps the list current: a new row she
/// cannot open is named in a delta although her rows did not change, a
/// change to rows she can read does not restate the list, and a row that
/// left the result (deleted) is no longer named.
///
/// ```text
/// alice ──sealed a──► server ◄──forged b── mallory
/// alice subscribes:   [a], left out: b
/// mallory forges d:   [a], left out: b, d   (a delta with no rows)
/// alice writes c:     [a, c]                (list unchanged, not restated)
/// mallory deletes b:  [a, c], left out: d
/// ```
#[test]
fn a_subscription_names_the_rows_it_left_out_and_keeps_the_list_current() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let a = sealed(&db, space, "a");
    pump(&server, &db);
    let b = forged(&server, space, "b");
    let query = Query::from("notes").order_by("title", OrderDirection::Asc);
    let mut stream =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);
    let mut told = Told::default();
    told.take(&mut stream);
    assert_eq!(told.rows, [a]);
    assert_eq!(told.unreadable_ids(), [b]);
    assert!(
        told.unreadable
            .iter()
            .all(|row| row.table == "notes" && row.code == "invalid-ciphertext"),
        "{:?}",
        told.unreadable
    );

    let d = forged(&server, space, "d");
    pump(&server, &db);
    let list_only = told.list_only;
    told.take(&mut stream);
    assert_eq!(told.rows, [a]);
    assert_eq!(told.unreadable_ids(), [b, d]);
    assert_eq!(
        told.list_only,
        list_only + 1,
        "the new list arrives though no row changed"
    );

    let c = sealed(&db, space, "c");
    let event = stream.try_next_event().expect("the readable row arrives");
    let SubscriptionEvent::Delta {
        added, unreadable, ..
    } = event
    else {
        panic!("{event:?}");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.row_uuid(), c);
    assert_eq!(unreadable, None, "the unreadable list has not changed");
    told.rows.insert(added[0].index, c);
    assert_eq!(told.rows, [a, c]);
    pump(&server, &db);
    told.take(&mut stream);

    block_on(server.delete("notes", b, Default::default())).unwrap();
    pump(&server, &db);
    told.take(&mut stream);
    assert_eq!(told.rows, [a, c]);
    assert_eq!(
        told.unreadable_ids(),
        [d],
        "a deleted row is no longer named"
    );
}

/// Contract: a page counts the rows it left out toward its limit, as a
/// one-shot read's does, and names them; when a write moves the window
/// past them, the page is filled from readable rows and the list empties.
///
/// ```text
/// alice ──sealed a, c──► server ◄──forged b── mallory
/// alice subscribes to 2 by title: [a], left out: b
/// alice writes "aa":              [a, aa], left out: none
/// ```
#[test]
fn a_page_counts_the_rows_it_left_out_and_names_them() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let a = sealed(&db, space, "a");
    sealed(&db, space, "c");
    pump(&server, &db);
    let b = forged(&server, space, "b");
    let query = Query::from("notes")
        .order_by("title", OrderDirection::Asc)
        .limit(2);
    let mut stream =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);
    let mut told = Told::default();
    told.take(&mut stream);
    assert_eq!(told.rows, [a], "the page is short by the row it left out");
    assert_eq!(told.unreadable_ids(), [b]);

    let aa = sealed(&db, space, "aa");
    told.take(&mut stream);
    assert_eq!(told.rows, [a, aa]);
    assert!(told.unreadable.is_empty(), "{:?}", told.unreadable);
}

/// Contract: a row sealed under a key alice does not hold is named
/// `key-pending` by her subscription; once she holds the key and the
/// runtime refreshes the reads of its table, the row arrives as an
/// addition and the list empties in the same delta.
///
/// ```text
/// writer ──sealed under v1──► server ──► alice: [], left out: row (key-pending)
/// alice keeps v1, reads refresh:         [row], left out: none
/// ```
#[test]
fn a_row_whose_key_arrives_leaves_the_list_as_it_joins_the_result() {
    let server = authority();
    let writer = open_client(Rc::new(MemoryStore::new()));
    let (writer_db, _w, _) = online(&writer, &server, None);
    let space = project(&writer_db, "plans");
    let key = new_space_key(&writer.device().unwrap(), &address(&writer_db, space));
    writer.record_fixture_rotation_check(key.address());
    block_on(writer.keep(key.clone())).unwrap();
    let row = block_on(writer_db.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    pump(&server, &writer_db);

    let alice = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&alice, &server, None);
    let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
    let mut stream =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);
    let mut told = Told::default();
    told.take(&mut stream);
    assert!(told.rows.is_empty());
    assert_eq!(told.unreadable_ids(), [row]);
    assert_eq!(told.unreadable[0].code, "key-pending");

    block_on(alice.keep(key)).unwrap();
    jazz::db::block_on(db.refresh_reads_of(&["notes"])).unwrap();
    let event = stream.try_next_event().expect("the row arrives");
    let SubscriptionEvent::Delta {
        added, unreadable, ..
    } = event
    else {
        panic!("{event:?}");
    };
    assert_eq!(
        added
            .iter()
            .map(|row| row.row.row_uuid())
            .collect::<Vec<_>>(),
        [row]
    );
    assert_eq!(
        unreadable.as_deref(),
        Some(&[][..]),
        "the delta that adds the row also says nothing is left out now"
    );
}

//! Latest-only tables (SPEC 4 §4.9, `INV-HIST-26`): a node keeps each row's
//! newest accepted version and the one it just superseded, and drops older
//! ones as it writes or receives the next, while a full-history table keeps
//! them all. Reads, subscriptions and rebases behave as they do on a full
//! table. Chunked values, which name versions a latest-only table drops,
//! are refused.
//!
//! The pruning itself has no effect a public API shows yet (history reads
//! are refused on latest-only tables), so the tests that pin it count the
//! stored history rows in the SQLite file, as the storage tests do.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, SubscriptionEvent, SyncMessage,
    Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::large_values::INLINE_VALUE_MAX_BYTES;
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

/// `presence(doc, cursor)`, latest-only, and `notes(doc, cursor)`, which
/// keeps full history, so each test compares the two.
fn schema() -> JazzSchema {
    let table = |name: &str| {
        TableSchemaBuilder::new(name)
            .column("doc", ColumnType::Text)
            .column("cursor", ColumnType::Text)
            .policies(allow_all_policies())
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(table("presence").latest_only())
            .table(table("notes"))
            .build(),
    )
}

fn open_at(node: u8, path: &Path) -> Db {
    block_on(Db::open_at(
        DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn authority_at(path: &Path) -> Db {
    let db = open_at(0, path);
    db.set_authority();
    db
}

struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn link(client: &Db, server: &Db) -> jazz::db::Upstream {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let link = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down,
                inbound: up,
            }),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    link
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..6 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn cells(doc: &str, cursor: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("doc".to_owned(), Value::String(doc.to_owned())),
        ("cursor".to_owned(), Value::String(cursor.to_owned())),
    ])
}

fn insert(db: &Db, table: &str, id: RowUuid, doc: &str, cursor: &str) {
    block_on(db.insert(
        table,
        cells(doc, cursor),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn update(db: &Db, table: &str, id: RowUuid, doc: &str, cursor: &str) {
    block_on(db.update(table, id, cells(doc, cursor), Default::default())).unwrap();
}

/// `(doc, cursor)` of each row of `table` in `doc` (every row for `None`).
fn read(db: &Db, table: &str, doc: Option<&str>) -> Vec<(String, String)> {
    let mut query = Query::from(table);
    if let Some(doc) = doc {
        query = query.filter(eq(col("doc"), lit(Value::String(doc.to_owned()))));
    }
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let text = |name: &str| match row.cells().find(|(cell, _)| *cell == name) {
                Some((_, Some(Value::String(text)))) => text.clone(),
                other => panic!("{name}: {other:?}"),
            };
            (text("doc"), text("cursor"))
        })
        .collect()
}

/// Accepted versions (`_n = 0`) in the history of stored table `table`.
fn history_rows(path: &Path, table: &str) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    let stored: String = conn
        .query_row(
            "SELECT stored_tbl FROM _jazz_names WHERE tbl = ?1 AND col = '' LIMIT 1",
            [table],
            |row| row.get(0),
        )
        .unwrap();
    conn.query_row(
        &format!("SELECT count(*) FROM \"{stored}__h\" WHERE _n = 0"),
        [],
        |row| row.get(0),
    )
    .unwrap()
}

/// Alice moves her cursor ten times on the authority. The latest-only
/// `presence` row keeps only the version its last write superseded; the
/// full-history `notes` row keeps all nine. Both read their latest value.
#[test]
fn the_authority_keeps_one_superseded_version_of_a_latest_only_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(&path);
    for table in ["presence", "notes"] {
        insert(&db, table, row(1), "a", "0");
        for step in 1..10 {
            update(&db, table, row(1), "a", &step.to_string());
        }
    }

    assert_eq!(read(&db, "presence", None), [("a".into(), "9".into())]);
    assert_eq!(read(&db, "notes", None), [("a".into(), "9".into())]);
    drop(db);
    assert_eq!(history_rows(&path, "presence"), 1);
    assert_eq!(history_rows(&path, "notes"), 9);
}

/// Alice watches the presence in doc `a` on the authority itself while her
/// row there moves a few times and then to `b`. The authority finds the
/// subscriptions a write touches from the row's new and previous images,
/// so the version each write supersedes must survive pruning: the row
/// leaves her result.
#[test]
fn a_row_leaves_a_keyed_subscription_on_the_authority() {
    let dir = tempfile::tempdir().unwrap();
    let db = authority_at(&dir.path().join("authority.sqlite"));
    insert(&db, "presence", row(1), "a", "0");
    let in_a = Query::from("presence").filter(eq(col("doc"), lit(Value::String("a".into()))));
    let mut stream =
        block_on(db.subscribe(&db.prepare_query(&in_a).unwrap(), ReadOpts::default())).unwrap();
    let mut ids = BTreeSet::new();
    let mut drain = |db: &Db, ids: &mut BTreeSet<RowUuid>| {
        block_on(db.tick()).unwrap();
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } = event
            {
                if reset {
                    ids.clear();
                }
                ids.extend(added.iter().map(|r| r.row.row_uuid()));
                for gone in removed {
                    ids.remove(&gone.row_uuid);
                }
            }
        }
    };
    drain(&db, &mut ids);
    assert_eq!(ids, BTreeSet::from([row(1)]));

    for step in 1..4 {
        update(&db, "presence", row(1), "a", &step.to_string());
        drain(&db, &mut ids);
    }
    update(&db, "presence", row(1), "b", "4");
    drain(&db, &mut ids);

    assert_eq!(ids, BTreeSet::new());
    assert_eq!(read(&db, "presence", Some("a")), []);
}

/// Bob watches the presence in doc `a` through the authority. Alice's row
/// moves to `b` and then to `c` there between Bob's syncs, after the
/// authority pruned its older versions, and the row leaves Bob's result;
/// his store keeps at most one superseded version of it too.
///
/// ```
/// alice@server ──a→b──b→c──► server ──► bob (doc = a): []
/// ```
#[test]
fn a_row_leaves_a_keyed_subscription_after_its_old_versions_are_pruned() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(&dir.path().join("server.sqlite"));
    let bob_path = dir.path().join("bob.sqlite");
    let bob = open_at(2, &bob_path);
    let _link = link(&bob, &server);
    insert(&server, "presence", row(1), "a", "0");
    let in_a = Query::from("presence").filter(eq(col("doc"), lit(Value::String("a".into()))));
    let watched = bob.prepare_query(&in_a).unwrap();
    let _sub = block_on(bob.subscribe(&watched, ReadOpts::default())).unwrap();
    pump(&server, &[&bob]);
    assert_eq!(
        read(&bob, "presence", Some("a")),
        [("a".into(), "0".into())]
    );

    for step in 1..4 {
        update(&server, "presence", row(1), "a", &step.to_string());
        pump(&server, &[&bob]);
    }
    update(&server, "presence", row(1), "b", "4");
    update(&server, "presence", row(1), "c", "5");
    pump(&server, &[&bob]);

    assert_eq!(read(&bob, "presence", Some("a")), []);
    drop((_sub, _link, bob));
    assert!(history_rows(&bob_path, "presence") <= 1);
}

/// Alice and Bob both move the same presence row while disconnected, Alice
/// several times. When they sync, Alice's pending writes rebase over Bob's
/// accepted one and the row converges to the write the authority sequenced
/// last on both; each store keeps at most one superseded accepted version.
#[test]
fn pending_writes_rebase_over_a_pruned_latest_only_row() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(&dir.path().join("server.sqlite"));
    let alice_path = dir.path().join("alice.sqlite");
    let bob_path = dir.path().join("bob.sqlite");
    let alice = open_at(1, &alice_path);
    let bob = open_at(2, &bob_path);
    let _alice_link = link(&alice, &server);
    let _bob_link = link(&bob, &server);
    let all = Query::from("presence");
    let _alice_sub =
        block_on(alice.subscribe(&alice.prepare_query(&all).unwrap(), ReadOpts::default()))
            .unwrap();
    let _bob_sub =
        block_on(bob.subscribe(&bob.prepare_query(&all).unwrap(), ReadOpts::default())).unwrap();
    insert(&server, "presence", row(1), "a", "0");
    for step in 1..4 {
        update(&server, "presence", row(1), "a", &step.to_string());
    }
    pump(&server, &[&alice, &bob]);

    update(&bob, "presence", row(1), "a", "bob");
    pump(&server, &[&bob]);
    for step in 0..3 {
        update(&alice, "presence", row(1), "a", &format!("alice {step}"));
    }
    assert_eq!(
        read(&alice, "presence", None),
        [("a".into(), "alice 2".into())]
    );
    pump(&server, &[&alice, &bob]);

    for db in [&server, &alice, &bob] {
        assert_eq!(read(db, "presence", None), [("a".into(), "alice 2".into())]);
    }
    drop((_alice_sub, _bob_sub, _alice_link, _bob_link, alice, bob));
    assert!(history_rows(&alice_path, "presence") <= 1);
    assert!(history_rows(&bob_path, "presence") <= 1);
}

/// A chunked value names the version it extends or proves access through,
/// which a latest-only table drops, so a value over the inline threshold
/// is refused there with a typed error; the same value fits a full table.
#[test]
fn a_chunked_value_is_refused_in_a_latest_only_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = authority_at(&dir.path().join("authority.sqlite"));
    let big = "x".repeat(INLINE_VALUE_MAX_BYTES + 1);
    let write = |table: &str| {
        block_on(db.insert(
            table,
            cells("a", &big),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
    };

    let error = write("presence").expect_err("a chunked value is refused");
    assert!(
        error
            .message
            .contains("in latest-only table `presence` is not supported yet"),
        "{}",
        error.message
    );
    assert!(read(&db, "presence", None).is_empty());
    write("notes").expect("a full-history table chunks it");
}

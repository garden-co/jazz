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
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

/// `presence(doc, cursor)`, latest-only, and `notes(doc, cursor)`, which
/// keeps full history, so each test compares the two.
fn schema() -> JazzSchema {
    schema_with(true)
}

/// [`schema`], with `presence` latest-only or not.
fn schema_with(latest_only: bool) -> JazzSchema {
    let table = |name: &str| {
        TableSchemaBuilder::new(name)
            .column("doc", ColumnType::Text)
            .column("cursor", ColumnType::Text)
            .policies(allow_all_policies())
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(match latest_only {
                true => table("presence").latest_only(),
                false => table("presence"),
            })
            .table(table("notes"))
            .build(),
    )
}

fn open_at(node: u8, path: &Path) -> Db {
    open_with(node, path, schema())
}

fn open_with(node: u8, path: &Path, schema: JazzSchema) -> Db {
    block_on(Db::open_at(
        DbConfig::new(
            schema,
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

/// Accepted versions (`_writer` empty, `_clock` below the pending kind) in the history of stored table `table`.
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
        &format!(
            "SELECT count(*) FROM \"{stored}__h\" WHERE _writer = X'' AND _clock BETWEEN 0 AND {}",
            (1i64 << 52) - 1
        ),
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

/// The authority admits two versions of the schema: one keeps
/// `presence`'s history, the other makes it latest-only. Lena's device
/// runs the latest-only version, Fran's the full one (a deployment rolled
/// back, say). Each write prunes by the version its writer runs: Lena's
/// drop the versions they supersede, Fran's keep them, also after a
/// restart. Reads of the table's history stay refused: what was pruned
/// stays pruned.
///
/// ```
/// lena (latest) ──a:0..4──► server ◄──a:5..8── fran (full)
/// server history of row 1:  1 version     →   5 versions
/// ```
#[test]
fn each_write_prunes_by_the_version_its_writer_runs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let server = open_with(0, &path, schema_with(false));
    server.set_authority();
    block_on(server.admit_schema_with_lenses(schema(), &[])).unwrap();
    let lena = open_with(1, &dir.path().join("lena.sqlite"), schema());
    let fran = open_with(2, &dir.path().join("fran.sqlite"), schema_with(false));
    let _lena_link = link(&lena, &server);
    let _fran_link = link(&fran, &server);
    let all = Query::from("presence");
    let _lena_sub =
        block_on(lena.subscribe(&lena.prepare_query(&all).unwrap(), ReadOpts::default())).unwrap();
    let _fran_sub =
        block_on(fran.subscribe(&fran.prepare_query(&all).unwrap(), ReadOpts::default())).unwrap();
    pump(&server, &[&lena, &fran]);

    insert(&lena, "presence", row(1), "a", "0");
    pump(&server, &[&lena, &fran]);
    for step in 1..5 {
        update(&lena, "presence", row(1), "a", &step.to_string());
        pump(&server, &[&lena, &fran]);
    }
    assert_eq!(history_rows(&path, "presence"), 1);

    for step in 5..9 {
        update(&fran, "presence", row(1), "a", &step.to_string());
        pump(&server, &[&lena, &fran]);
    }
    assert_eq!(history_rows(&path, "presence"), 5);
    // The dedupe entries alike: Lena's device keeps its newest, Fran's all.
    assert_eq!(transactions_of(&path, 1), 1);
    assert_eq!(transactions_of(&path, 2), 4);
    assert_eq!(read(&lena, "presence", None), [("a".into(), "8".into())]);

    let history = Query::from("history.presence").filter(eq(col("id"), lit(Value::Uuid(row(1).0))));
    let error = server
        .prepare_query(&history)
        .and_then(|prepared| block_on(server.all(&prepared, ReadOpts::default())))
        .expect_err("a table some version keeps latest-only has no history to read");
    assert!(
        error.message.contains("latest-only table `presence`"),
        "{}",
        error.message
    );
    drop(server);
    let server = open_with(0, &path, schema_with(false));
    server.set_authority();
    update(&server, "presence", row(1), "a", "9");
    drop(server);
    assert_eq!(history_rows(&path, "presence"), 6);
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

/// Alice and Bob each move the same row from the same version while
/// disconnected; Bob's write lands first, so Alice's overrides a version
/// she never saw. On the full-history `notes` table the authority reports
/// her write as maybe conflicting; on the latest-only `presence` table,
/// whose history no longer holds what she missed, it reads as blind.
#[test]
fn a_write_to_a_latest_only_table_is_never_maybe_conflicting() {
    for (table, expected) in [("notes", true), ("presence", false)] {
        let dir = tempfile::tempdir().unwrap();
        let server = authority_at(&dir.path().join("server.sqlite"));
        let alice = open_at(1, &dir.path().join("alice.sqlite"));
        let bob = open_at(2, &dir.path().join("bob.sqlite"));
        let _alice_link = link(&alice, &server);
        let _bob_link = link(&bob, &server);
        let all = Query::from(table);
        let _subs = [&alice, &bob].map(|db| {
            block_on(db.subscribe(&db.prepare_query(&all).unwrap(), ReadOpts::default())).unwrap()
        });
        insert(&server, table, row(1), "a", "0");
        pump(&server, &[&alice, &bob]);

        update(&bob, table, row(1), "a", "bob");
        pump(&server, &[&bob]);
        update(&alice, table, row(1), "a", "alice");
        pump(&server, &[&alice, &bob]);

        let conflicts = server.write_conflicts(table, row(1), None).unwrap();
        let last = conflicts.last().unwrap();
        assert_eq!(last.maybe_conflicting, expected, "{table}: {conflicts:?}");
        assert_eq!(read(&server, table, None), [("a".into(), "alice".into())]);
    }
}

/// History reads need every version, which a latest-only table drops, so
/// `history.presence` is refused with a typed error; `history.notes` lists
/// each version.
#[test]
fn history_reads_of_a_latest_only_table_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = authority_at(&dir.path().join("authority.sqlite"));
    for table in ["presence", "notes"] {
        insert(&db, table, row(1), "a", "0");
        update(&db, table, row(1), "a", "1");
    }
    let history = |table: &str| {
        let query = Query::from(format!("history.{table}").as_str())
            .filter(eq(col("id"), lit(Value::Uuid(row(1).0))));
        db.prepare_query(&query)
            .and_then(|prepared| block_on(db.all(&prepared, ReadOpts::default())))
    };

    let error = history("presence").expect_err("history of a latest-only table");
    assert!(
        error
            .message
            .contains("reading the history of latest-only table `presence` is not supported yet"),
        "{}",
        error.message
    );
    assert_eq!(history("notes").unwrap().len(), 2);
}

/// A serializable transaction reads at its snapshot, which may name a
/// version a latest-only table already dropped, so its read of `presence`
/// is refused; the same read of `notes` works.
#[test]
fn a_serializable_read_of_a_latest_only_table_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = authority_at(&dir.path().join("authority.sqlite"));
    for table in ["presence", "notes"] {
        insert(&db, table, row(1), "a", "0");
    }
    let read_in_tx = |table: &str| {
        let open = OpenTransactionId::new();
        block_on(db.begin_serializable(open)).unwrap();
        let prepared = db.prepare_query(&Query::from(table)).unwrap();
        block_on(db.tx_ref(open).all_prepared(&prepared)).map(|rows| rows.len())
    };

    let error = read_in_tx("presence").expect_err("a snapshot read of a latest-only table");
    assert!(
        error
            .message
            .contains("reading latest-only table `presence` at a snapshot is not supported yet"),
        "{}",
        error.message
    );
    assert_eq!(read_in_tx("notes").unwrap(), 1);
}

/// Bob watches the presence in doc `a`, goes offline, and Alice's row
/// moves to `b` on the authority while he is away. Resuming would read
/// his prior result as of when he left, a version the table dropped, so
/// the authority opens his subscription fresh instead: the row leaves his
/// result, and his subscription stays live when it moves back.
#[test]
fn a_subscription_over_a_moved_latest_only_table_resumes_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(&dir.path().join("server.sqlite"));
    let bob = open_at(2, &dir.path().join("bob.sqlite"));
    let upstream = link(&bob, &server);
    insert(&server, "presence", row(1), "a", "0");
    let in_a = Query::from("presence").filter(eq(col("doc"), lit(Value::String("a".into()))));
    let mut stream =
        block_on(bob.subscribe(&bob.prepare_query(&in_a).unwrap(), ReadOpts::default())).unwrap();
    pump(&server, &[&bob]);
    assert_eq!(
        read(&bob, "presence", Some("a")),
        [("a".into(), "0".into())]
    );
    let mut refused = || {
        std::iter::from_fn(|| stream.try_next_event())
            .filter(|event| matches!(event, SubscriptionEvent::Rejected { .. }))
            .count()
    };
    assert_eq!(refused(), 0);

    assert!(bob.detach_connection(&upstream));
    for step in 1..4 {
        update(&server, "presence", row(1), "a", &step.to_string());
    }
    update(&server, "presence", row(1), "b", "4");
    let _upstream = link(&bob, &server);
    pump(&server, &[&bob]);

    assert_eq!(read(&bob, "presence", Some("a")), []);
    assert_eq!(read(&bob, "presence", None), []);
    update(&server, "presence", row(1), "a", "5");
    pump(&server, &[&bob]);
    assert_eq!(
        read(&bob, "presence", Some("a")),
        [("a".into(), "5".into())]
    );
    assert_eq!(refused(), 0, "the resume is not refused");
}

/// Accepted commits node `node` uploaded that the authority at `path`
/// still keeps a dedupe entry for.
fn transactions_of(path: &Path, node: u8) -> i64 {
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM _jazz_transactions AS t JOIN _jazz_nodes AS n ON n.n = t.n \
             WHERE n.node = ?1 AND t.t BETWEEN 0 AND (1 << 52) - 1",
            [vec![node; 16]],
            |row| row.get(0),
        )
        .unwrap()
}

/// [`link`], also handing back the queue of what the server sends the
/// client, so a test can lose it.
fn lossy_link(
    client: &Db,
    server: &Db,
) -> (jazz::db::Upstream, Rc<RefCell<VecDeque<SyncMessage>>>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let link = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down.clone(),
                inbound: up,
            }),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    (link, down)
}

/// Alice moves her presence ten times, one upload each, and also edits a
/// note. The authority keeps a dedupe entry for her newest presence-only
/// commit alone, and one for every commit that wrote `notes`.
#[test]
fn the_authority_keeps_one_dedupe_entry_per_device_for_latest_only_commits() {
    let dir = tempfile::tempdir().unwrap();
    let server_path = dir.path().join("server.sqlite");
    let server = authority_at(&server_path);
    let alice = open_at(1, &dir.path().join("alice.sqlite"));
    let _link = link(&alice, &server);
    insert(&alice, "presence", row(1), "a", "0");
    pump(&server, &[&alice]);
    for step in 1..10 {
        update(&alice, "presence", row(1), "a", &step.to_string());
        pump(&server, &[&alice]);
    }
    drop(server);
    let server = authority_at(&server_path);
    assert_eq!(transactions_of(&server_path, 1), 1);

    let _link = link(&alice, &server);
    insert(&alice, "notes", row(2), "a", "note");
    pump(&server, &[&alice]);
    update(&alice, "presence", row(1), "a", "10");
    pump(&server, &[&alice]);
    update(&alice, "notes", row(2), "a", "edited");
    pump(&server, &[&alice]);

    assert_eq!(read(&server, "presence", None), [("a".into(), "10".into())]);
    assert_eq!(alice.pending_upload_count(), 0);
    drop(server);
    // The note's two commits; the presence commit between them went
    // once the note's edit followed it.
    assert_eq!(transactions_of(&server_path, 1), 2);
}

/// Offline, Alice moves her cursor in doc `a`, her row in doc `b`, then her
/// cursor in `a` again: the third write was built on the first, whose
/// dedupe entry the authority drops when it accepts the second. It is
/// accepted as a blind write, and all three converge.
#[test]
fn a_write_built_on_a_commit_whose_entry_was_dropped_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(&dir.path().join("server.sqlite"));
    let alice = open_at(1, &dir.path().join("alice.sqlite"));
    let _link = link(&alice, &server);
    insert(&server, "presence", row(1), "a", "0");
    insert(&server, "presence", row(2), "b", "0");
    let all = Query::from("presence");
    let _sub = block_on(alice.subscribe(&alice.prepare_query(&all).unwrap(), ReadOpts::default()))
        .unwrap();
    pump(&server, &[&alice]);

    update(&alice, "presence", row(1), "a", "1");
    update(&alice, "presence", row(2), "b", "1");
    update(&alice, "presence", row(1), "a", "2");
    pump(&server, &[&alice]);

    let expected: Vec<(String, String)> = vec![("a".into(), "2".into()), ("b".into(), "1".into())];
    assert_eq!(read(&server, "presence", None), expected);
    assert_eq!(read(&alice, "presence", None), expected);
    assert_eq!(alice.pending_upload_count(), 0);
}

/// Alice uploads two cursor moves, but their fates are lost and she
/// reconnects. Her first commit's dedupe entry is gone, but her second's
/// shows the first came before it, so its resend is answered as accepted
/// rather than applied again over the second: the row stays at her newest
/// write on both sides, and the authority still keeps one entry for her.
#[test]
fn a_resent_commit_whose_entry_was_dropped_is_not_applied_again() {
    let dir = tempfile::tempdir().unwrap();
    let server_path = dir.path().join("server.sqlite");
    let server = authority_at(&server_path);
    let alice = open_at(1, &dir.path().join("alice.sqlite"));
    let (upstream, down) = lossy_link(&alice, &server);
    insert(&alice, "presence", row(1), "a", "0");
    pump(&server, &[&alice]);
    update(&alice, "presence", row(1), "a", "1");
    update(&alice, "presence", row(1), "a", "2");
    block_on(alice.tick()).unwrap();
    block_on(server.tick()).unwrap();
    down.borrow_mut().clear();
    assert!(alice.detach_connection(&upstream));
    assert_eq!(read(&server, "presence", None), [("a".into(), "2".into())]);
    assert!(alice.pending_upload_count() > 0, "the fates were lost");

    let _link = link(&alice, &server);
    pump(&server, &[&alice]);

    assert_eq!(read(&server, "presence", None), [("a".into(), "2".into())]);
    assert_eq!(read(&alice, "presence", None), [("a".into(), "2".into())]);
    assert_eq!(alice.pending_upload_count(), 0);
    drop(server);
    assert_eq!(transactions_of(&server_path, 1), 1);
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

/// Alice's store holds the server's new `presence` and `notes` rows, but
/// not yet the connection's position past them, as a sharded upstream
/// can leave it. A one-shot read of `notes` reads as of the position she
/// knows, so it leaves the new note out; `presence` keeps no version to
/// read back to, so it reads its rows as she holds them (a branch view's
/// known limit, `INV-HIST-29`) rather than being refused.
#[test]
fn a_latest_only_read_skips_the_per_query_position() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(&dir.path().join("server.sqlite"));
    let alice = open_at(1, &dir.path().join("alice.sqlite"));
    let (_upstream, down) = lossy_link(&alice, &server);
    let live = |table: &str| {
        block_on(alice.subscribe(
            &alice.prepare_query(&Query::from(table)).unwrap(),
            ReadOpts::default(),
        ))
        .unwrap()
    };
    let (_presence, _notes) = (live("presence"), live("notes"));
    pump(&server, &[&alice]);

    insert(&server, "presence", row(1), "a", "0");
    insert(&server, "notes", row(2), "a", "note");
    for _ in 0..6 {
        block_on(server.tick()).unwrap();
        down.borrow_mut()
            .retain(|message| !matches!(message, SyncMessage::ConnectionSettled { .. }));
        block_on(alice.tick()).unwrap();
    }

    let one_shot = |table: &str| {
        let prepared = alice.prepare_query(&Query::from(table)).unwrap();
        block_on(alice.all(&prepared, ReadOpts::default()))
    };
    assert_eq!(
        one_shot("notes").unwrap().len(),
        0,
        "the note is past the position Alice knows"
    );
    assert_eq!(read(&alice, "presence", None), [("a".into(), "0".into())]);
}

/// A cross-shard commit's part reaches this shard only after two later
/// commits of the same device, which wrote only this shard and landed
/// first: a node's commits do not arrive in order across shards. The
/// second of them dropped the first's dedupe entry, so the part's tx is
/// below one this shard dropped, yet the part is held here, so it is
/// applied at its fixed position rather than answered as a dropped
/// resend. A part's entry is never dropped: it records its cross-shard
/// commit's fate.
///
/// ```
/// alice ──part(row 2)──┐ (held back)
/// alice ──c2(row 1)──c3(row 1)──► shard   then   part ──► shard: applied
/// ```
#[test]
fn a_late_cross_shard_part_is_applied_not_taken_for_a_dropped_resend() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let now = Rc::new(std::cell::Cell::new(1_790_000_000_000u64));
    let clock = now.clone();
    let shard = block_on(Db::open_at(
        DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([0; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_clock(move || clock.get()),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    shard.set_authority();
    assert!(shard.shard().is_shard());
    let alice = open_at(1, &dir.path().join("alice.sqlite"));
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let _link = block_on(alice.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    shard
        .accept_subscriber(
            Box::new(Pipe {
                out: down,
                inbound: up.clone(),
            }),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    insert(&alice, "presence", row(1), "a", "0");
    pump(&shard, &[&alice]);

    // The part: Alice's insert of row 2, held back on its way.
    insert(&alice, "presence", row(2), "b", "0");
    block_on(alice.tick()).unwrap();
    let position = up
        .borrow()
        .iter()
        .position(|m| matches!(m, SyncMessage::Commit { .. }))
        .expect("the part was uploaded");
    let late = up.borrow_mut().remove(position).unwrap();
    let SyncMessage::Commit { id, .. } = &late else {
        unreachable!()
    };
    let part = jazz::db::shard::CommitRef::from(id);
    let held = shard.shard().hold(part, &[]).unwrap();
    shard.shard().fix(part, held).unwrap();

    for step in 1..3 {
        update(&alice, "presence", row(1), "a", &step.to_string());
        pump(&shard, &[&alice]);
    }
    assert_eq!(read(&shard, "presence", None), [("a".into(), "2".into())]);

    up.borrow_mut().push_back(late);
    pump(&shard, &[&alice]);

    assert_eq!(shard.shard().lowest_hold(), None, "the part landed");
    assert_eq!(
        read(&shard, "presence", Some("b")),
        [("b".into(), "0".into())],
        "and was applied"
    );
    assert_eq!(alice.pending_upload_count(), 0);
    drop(shard);
    // The part's entry, and Alice's newest commit's.
    assert_eq!(transactions_of(&path, 1), 2);
}

/// Alice's device loses the tail of its file: it reopens from a copy made
/// before her last few cursor moves, whose dedupe entries the authority
/// had dropped. Her next move is numbered past them all, by her clock
/// (INV-EDGE-28), so it is applied rather than taken for a resend of a
/// dropped commit.
#[test]
fn a_write_after_a_lost_tail_is_not_taken_for_a_dropped_resend() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(&dir.path().join("server.sqlite"));
    let alice_path = dir.path().join("alice.sqlite");
    let backup = dir.path().join("backup");
    std::fs::create_dir(&backup).unwrap();
    let copy = |from: &Path, to: &Path| {
        for suffix in ["", "-wal", "-shm"] {
            let source = Path::new(&format!("{}{suffix}", from.display())).to_path_buf();
            let target = Path::new(&format!("{}{suffix}", to.display())).to_path_buf();
            let _ = std::fs::remove_file(&target);
            if source.exists() {
                std::fs::copy(&source, &target).unwrap();
            }
        }
    };
    let alice = open_at(1, &alice_path);
    let link_alice = link(&alice, &server);
    insert(&alice, "presence", row(1), "a", "0");
    pump(&server, &[&alice]);
    drop((link_alice, alice));
    copy(&alice_path, &backup.join("alice.sqlite"));

    let alice = open_at(1, &alice_path);
    let link_alice = link(&alice, &server);
    for step in 1..5 {
        update(&alice, "presence", row(1), "a", &step.to_string());
        pump(&server, &[&alice]);
    }
    assert_eq!(read(&server, "presence", None), [("a".into(), "4".into())]);
    drop((link_alice, alice));

    std::thread::sleep(std::time::Duration::from_millis(5));
    copy(&backup.join("alice.sqlite"), &alice_path);
    let alice = open_at(1, &alice_path);
    let _link = link(&alice, &server);
    let all = Query::from("presence");
    let _sub = block_on(alice.subscribe(&alice.prepare_query(&all).unwrap(), ReadOpts::default()))
        .unwrap();
    pump(&server, &[&alice]);
    update(&alice, "presence", row(1), "a", "after");
    pump(&server, &[&alice]);

    assert_eq!(
        read(&server, "presence", None),
        [("a".into(), "after".into())]
    );
    assert_eq!(alice.pending_upload_count(), 0);
}

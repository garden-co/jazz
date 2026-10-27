//! Chunked large values over sync: row images carry descriptors, replicas
//! pull chunks by locator (prefetching indexed columns before the row is
//! installed), and clients push the nodes of their new values ahead of the
//! commit that references them (`SPEC/19_large_values.md`).

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SubscriptionStream, SyncMessage,
    Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

const LARGE: usize = 300 * 1024;

fn open(node: u8, author: AuthorSubject) -> Db {
    let docs = TableSchemaBuilder::new("docs")
        .column("body", ColumnType::Text)
        .column("blob", ColumnType::Bytea)
        .policies(allow_all_policies());
    let titled = TableSchemaBuilder::new("titled")
        .column("title", ColumnType::Text)
        .index_only(["title"])
        .policies(allow_all_policies());
    // Readable while any doc exists: its read policy reads `docs`.
    let notes = TableSchemaBuilder::new("notes")
        .column("text", ColumnType::Text)
        .policies(allow_all_policies().with_select(PolicyExpr::Exists {
            table: "docs".into(),
            condition: Box::new(PolicyExpr::True),
        }));
    let config = DbConfig::new(
        compile_schema(
            &SchemaBuilder::new()
                .table(docs)
                .table(titled)
                .table(notes)
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    block_on(Db::open(config)).unwrap()
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn text(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'a' + (state % 26) as u8) as char
        })
        .collect()
}

/// A duplex that counts the chunk traffic it carries.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    log: Rc<RefCell<Vec<&'static str>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let kind = match &message {
            SyncMessage::ChunkRequests { .. } => "requests",
            SyncMessage::PushChunks { .. } => "push",
            SyncMessage::Chunks { .. } => "chunks",
            SyncMessage::Commit { .. } => "commit",
            _ => "other",
        };
        self.log.borrow_mut().push(kind);
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

type Log = Rc<RefCell<Vec<&'static str>>>;

/// Connect `client` to `server` as `identity`; the log records what both
/// sides sent, in order.
fn connect(server: &Db, client: &Db, identity: AuthorSubject) -> Log {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let log = Rc::new(RefCell::new(Vec::new()));
    let _ = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
        log: log.clone(),
    })));
    server.accept_subscriber(
        Box::new(Pipe {
            out: down,
            inbound: up,
            log: log.clone(),
        }),
        identity,
    );
    log
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..12 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

fn subscribe(db: &Db, query: Query) -> SubscriptionStream {
    block_on(db.subscribe(&db.prepare_query(&query).unwrap(), ReadOpts::default())).unwrap()
}

fn local_cell(db: &Db, table: &str, n: u8, column: &str) -> Result<Option<Value>, jazz::db::Error> {
    let query = Query::from(table).filter(eq(col("id"), lit(Value::Uuid(row(n).0))));
    let rows = block_on(db.all(
        &db.prepare_query(&query).unwrap(),
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))?;
    Ok(rows.first().and_then(|row| {
        row.cells()
            .find(|(name, _)| *name == column)
            .and_then(|(_, value)| value.cloned())
    }))
}

fn insert(db: &Db, table: &str, n: u8, cells: Vec<(&str, Value)>) {
    block_on(
        db.insert(
            table,
            cells
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect(),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ),
    )
    .unwrap();
}

#[test]
fn a_replica_pulls_the_chunks_its_results_need() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let log = connect(&server, &alice, user(1));
    let body = text(1, LARGE);
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String(body.clone())),
            ("blob", Value::Bytes(vec![1, 2, 3])),
        ],
    );
    let mut stream = subscribe(&alice, Query::from("docs"));
    pump(&server, &[&alice]);
    assert!(log.borrow().contains(&"requests"), "{:?}", log.borrow());
    assert_eq!(
        local_cell(&alice, "docs", 1, "body").unwrap(),
        Some(Value::String(body.clone()))
    );
    // The subscription published the whole value, never a missing-chunk
    // rejection.
    let mut saw = false;
    while let Some(event) = stream.try_next_event() {
        let text = format!("{event:?}");
        assert!(!text.contains("Rejected"), "{text}");
        saw |= text.contains(&body[..64]);
    }
    assert!(saw, "the opening carried the value");
}

#[test]
fn a_range_read_on_a_replica_fetches_only_what_it_covers() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let _log = connect(&server, &alice, user(1));
    let blob: Vec<u8> = (0..LARGE * 4).map(|n| (n * 7 % 251) as u8).collect();
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String("small".into())),
            ("blob", Value::Bytes(blob.clone())),
        ],
    );
    // A query that projects only the small column installs the row
    // without any of the blob's chunks.
    let _stream = subscribe(&alice, Query::from("docs").select(["body"]));
    pump(&server, &[&alice]);
    let (before, _) = alice.large_value_nodes_for_test().unwrap();
    assert_eq!(before, 0, "nothing prefetched for an unindexed column");
    let range = 600_000..600_100;
    let first = block_on(alice.read_value_range("docs", row(1), "blob", range.clone()));
    assert!(first.is_err(), "the first read asks for the chunks");
    pump(&server, &[&alice]);
    let mut got = block_on(alice.read_value_range("docs", row(1), "blob", range.clone()));
    for _ in 0..4 {
        if got.is_ok() {
            break;
        }
        pump(&server, &[&alice]);
        got = block_on(alice.read_value_range("docs", row(1), "blob", range.clone()));
    }
    assert_eq!(got.unwrap(), blob[600_000..600_100]);
    let (after, _) = alice.large_value_nodes_for_test().unwrap();
    let (total, _) = server.large_value_nodes_for_test().unwrap();
    assert!(after < total, "only the covering path: {after} of {total}");
}

#[test]
fn indexed_large_values_are_prefetched_before_the_row_is_installed() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let _log = connect(&server, &alice, user(1));
    let base = text(2, LARGE);
    let titles = [format!("{base}b"), format!("{base}a"), "short".to_owned()];
    for (n, title) in titles.iter().enumerate() {
        insert(
            &server,
            "titled",
            n as u8 + 1,
            vec![("title", Value::String(title.clone()))],
        );
    }
    let _stream = subscribe(&alice, Query::from("titled"));
    pump(&server, &[&alice]);
    // Every chunk is local, so ordering through the index never waits.
    let (nodes, _) = alice.large_value_nodes_for_test().unwrap();
    assert_eq!(nodes, server.large_value_nodes_for_test().unwrap().0);
    let query = Query::from("titled").order_by("title", OrderDirection::Asc);
    let rows = block_on(alice.all(
        &alice.prepare_query(&query).unwrap(),
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    let ids: Vec<RowUuid> = rows.iter().map(|r| r.row_uuid()).collect();
    assert_eq!(ids, vec![row(2), row(1), row(3)]);
}

#[test]
fn client_edits_push_their_new_nodes_before_the_commit() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let bob = open(2, user(2));
    let log = connect(&server, &alice, user(1));
    let _bob_log = connect(&server, &bob, user(2));
    let _bob_stream = subscribe(&bob, Query::from("docs"));
    // Alice shows what she edits, so her replica holds its chunks.
    let _alice_stream = subscribe(&alice, Query::from("docs"));
    let mut expected = text(3, LARGE);
    insert(
        &alice,
        "docs",
        1,
        vec![
            ("body", Value::String(expected.clone())),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    pump(&server, &[&alice, &bob]);
    // Appends add tail entries; past the tail's bound the client
    // consolidates, and the new nodes go up ahead of that commit.
    for n in 0..70 {
        let suffix = format!("<{n}>");
        block_on(alice.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
        expected.push_str(&suffix);
        if n % 10 == 0 {
            pump(&server, &[&alice, &bob]);
        }
    }
    pump(&server, &[&alice, &bob]);
    let log = log.borrow();
    let push = log.iter().position(|k| *k == "push").expect("a push");
    assert!(
        log[push..].contains(&"commit"),
        "the push precedes a commit: {log:?}"
    );
    for db in [&server, &alice, &bob] {
        assert_eq!(
            local_cell(db, "docs", 1, "body").unwrap(),
            Some(Value::String(expected.clone()))
        );
    }
}

#[test]
fn a_streamed_upload_reaches_other_replicas() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let bob = open(2, user(2));
    let _a = connect(&server, &alice, user(1));
    let _b = connect(&server, &bob, user(2));
    let _bob_stream = subscribe(&bob, Query::from("docs"));
    let blob: Vec<u8> = (0..LARGE * 2).map(|n| (n * 13 % 253) as u8).collect();
    block_on(alice.insert_streaming_value_with_id(
        "docs",
        row(1),
        BTreeMap::from([("body".to_owned(), Value::String("upload".into()))]),
        "blob",
        std::io::Cursor::new(blob.clone()),
    ))
    .unwrap();
    pump(&server, &[&alice, &bob]);
    assert_eq!(
        local_cell(&server, "docs", 1, "blob").unwrap(),
        Some(Value::Bytes(blob.clone()))
    );
    assert_eq!(
        local_cell(&bob, "docs", 1, "blob").unwrap(),
        Some(Value::Bytes(blob))
    );
}

/// A streamed value small enough to be stored inline still reaches the
/// server as an ordinary row.
#[test]
fn a_small_streamed_upload_reaches_the_server() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let _a = connect(&server, &alice, user(1));
    block_on(alice.insert_streaming_value_with_id(
        "docs",
        row(1),
        BTreeMap::from([("body".to_owned(), Value::String("upload".into()))]),
        "blob",
        std::io::Cursor::new(vec![1, 2, 3, 4, 5, 6]),
    ))
    .unwrap();
    pump(&server, &[&alice]);
    assert_eq!(
        local_cell(&server, "docs", 1, "blob").unwrap(),
        Some(Value::Bytes(vec![1, 2, 3, 4, 5, 6]))
    );
}

/// A replica streams a small value before it first connects: the chunks
/// go up ahead of the commit once it does, and the server takes the row.
#[test]
fn a_small_value_streamed_before_connecting_reaches_the_server() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    alice.become_replica();
    block_on(alice.insert_streaming_value_with_id(
        "docs",
        row(1),
        BTreeMap::from([("body".to_owned(), Value::String("upload".into()))]),
        "blob",
        std::io::Cursor::new(vec![1, 2, 3, 4, 5, 6]),
    ))
    .unwrap();
    // A tick offline reclaims what nothing references.
    block_on(alice.tick()).unwrap();
    let _a = connect(&server, &alice, user(1));
    pump(&server, &[&alice]);
    assert_eq!(
        local_cell(&server, "docs", 1, "blob").unwrap(),
        Some(Value::Bytes(vec![1, 2, 3, 4, 5, 6]))
    );
}

/// A trusted backend streams a value for a session it acts for, chunk by
/// chunk as the bindings do: the row reaches the server.
#[test]
fn a_backends_streamed_upload_for_a_session_reaches_the_server() {
    for blob in [
        vec![1, 2, 3, 4, 5, 6],
        (0..LARGE).map(|n| (n % 251) as u8).collect(),
    ] {
        let server = open(0, AuthorSubject::SYSTEM);
        let backend = open(1, AuthorSubject::SYSTEM);
        let _link = connect(&server, &backend, AuthorSubject::SYSTEM);
        let cells = BTreeMap::from([("body".to_owned(), Value::String("upload".into()))]);
        let mut upload = backend
            .begin_streaming_value_upload("docs", &cells, "blob")
            .unwrap();
        for chunk in blob.chunks(64 * 1024) {
            block_on(backend.push_streaming_value_upload(&mut upload, chunk)).unwrap();
        }
        block_on(backend.finish_streaming_value_upload(
            upload,
            jazz::db::StreamingMutationKind::Insert,
            "docs",
            row(1),
            cells,
            "blob",
            jazz::db::WriteIdentity::Session(user(5)),
            None,
            None,
            None,
        ))
        .unwrap();
        pump(&server, &[&backend]);
        assert_eq!(
            local_cell(&server, "docs", 1, "blob").unwrap(),
            Some(Value::Bytes(blob.clone())),
            "{} bytes",
            blob.len()
        );
    }
}

/// Alice's second large upload, held back by the server's ingress limit:
/// the first lands within the window, the second waits for a later one.
fn held_upload() -> (Db, Db, Log) {
    let server = open(0, AuthorSubject::SYSTEM);
    server.set_large_value_staging_policy(jazz::db::LargeValueStagingPolicy {
        incoming_bytes_per_window: LARGE as u64,
        window_ms: 60_000,
        max_age_ms: 10 * 60 * 1_000,
    });
    insert(
        &server,
        "titled",
        9,
        vec![("title", Value::String("on the server".into()))],
    );
    let alice = open(1, user(1));
    let log = connect(&server, &alice, user(1));
    pump(&server, &[&alice]);
    for n in [1, 2] {
        insert(
            &alice,
            "docs",
            n,
            vec![
                ("body", Value::String(text(u64::from(n), LARGE))),
                ("blob", Value::Bytes(Vec::new())),
            ],
        );
    }
    pump(&server, &[&alice]);
    assert!(local_cell(&server, "docs", 1, "body").unwrap().is_some());
    assert_eq!(
        local_cell(&server, "docs", 2, "body").unwrap(),
        None,
        "the second upload waits for the next ingress window"
    );
    (server, alice, log)
}

fn remote_subscribe(db: &Db, table: &str) -> SubscriptionStream {
    let query = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.subscribe(
        &query,
        ReadOpts {
            tier: ReadTier::Remote,
            ..ReadOpts::default()
        },
    ))
    .unwrap()
}

/// Applies a subscription's pending events to `shown`; whether the server
/// answered it.
fn drain(stream: &mut SubscriptionStream, shown: &mut BTreeSet<RowUuid>) -> bool {
    let mut answered = false;
    while let Some(event) = stream.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta {
            added,
            removed,
            settled,
            ..
        } = event
        {
            for gone in removed {
                shown.remove(&gone.row_uuid);
            }
            shown.extend(added.iter().map(|row| row.row.row_uuid()));
            answered |= settled;
        }
    }
    answered
}

/// A client uploading past the server's ingress limit can still read what
/// its held-back writes cannot change: a subscription to another table is
/// served now, while the upload waits.
///
/// ```text
/// alice ──push 2, commit 2(docs)──► held until the next window
/// alice ──subscribe(titled)───────► served, settles with the server's row
/// ```
#[test]
fn reads_of_other_tables_overtake_commits_held_by_the_ingress_limit() {
    let (server, alice, _log) = held_upload();
    let mut stream = remote_subscribe(&alice, "titled");
    pump(&server, &[&alice]);
    let mut shown = BTreeSet::new();
    assert!(
        drain(&mut stream, &mut shown),
        "the read is served while the upload is held"
    );
    assert_eq!(shown.into_iter().collect::<Vec<_>>(), [row(9)]);
    assert_eq!(local_cell(&server, "docs", 2, "body").unwrap(), None);
}

/// A read that could observe a write the ingress limit holds back waits for
/// it, and is answered with it once the upload is let through.
///
/// ```text
/// alice ──push 2, commit 2(docs)──► held
/// alice ──subscribe(docs)─────────► held behind commit 2
/// (limit lifted)                  ► commit 2, then the read: rows 1 and 2
/// ```
#[test]
fn reads_wait_for_commits_held_by_the_ingress_limit() {
    let (server, alice, _log) = held_upload();
    let mut stream = remote_subscribe(&alice, "docs");
    let mut shown = BTreeSet::new();
    pump(&server, &[&alice]);
    assert!(
        !drain(&mut stream, &mut shown),
        "the read waits behind the held commit"
    );
    assert_eq!(local_cell(&server, "docs", 2, "body").unwrap(), None);

    server.set_large_value_staging_policy(jazz::db::LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 10 * 60 * 1_000,
    });
    pump(&server, &[&alice]);
    assert!(local_cell(&server, "docs", 2, "body").unwrap().is_some());
    assert!(
        drain(&mut stream, &mut shown),
        "the read is answered once the upload lands"
    );
    assert_eq!(
        shown.into_iter().collect::<Vec<_>>(),
        [row(1), row(2)],
        "the read includes the writer's own upload"
    );
}

/// A read whose table's policy reads a table the held commits write waits
/// too: the held write could change which rows it may see.
#[test]
fn reads_whose_policy_reads_a_held_table_wait() {
    let (server, alice, _log) = held_upload();
    insert(
        &server,
        "notes",
        7,
        vec![("text", Value::String("note".into()))],
    );
    let mut stream = remote_subscribe(&alice, "notes");
    let mut shown = BTreeSet::new();
    pump(&server, &[&alice]);
    assert!(
        !drain(&mut stream, &mut shown),
        "the read waits behind the held commit its policy could observe"
    );
    server.set_large_value_staging_policy(jazz::db::LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 10 * 60 * 1_000,
    });
    pump(&server, &[&alice]);
    assert!(drain(&mut stream, &mut shown), "then it is answered");
    assert_eq!(shown.into_iter().collect::<Vec<_>>(), [row(7)]);
}

/// One session's held upload holds back only its own reads: another
/// session reading the same table is served from what the server has.
#[test]
fn a_held_upload_does_not_hold_other_sessions_reads() {
    let (server, alice, _log) = held_upload();
    let bob = open(2, user(2));
    let _bob = connect(&server, &bob, user(2));
    let mut stream = remote_subscribe(&bob, "docs");
    pump(&server, &[&alice, &bob]);
    let mut shown = BTreeSet::new();
    assert!(drain(&mut stream, &mut shown), "bob's read is served");
    assert_eq!(shown.into_iter().collect::<Vec<_>>(), [row(1)]);
    assert_eq!(local_cell(&server, "docs", 2, "body").unwrap(), None);
}

//! Chunked large values over sync: row images carry descriptors, replicas
//! pull chunks (prefetching indexed columns before the row is
//! installed), and clients push the nodes of their new values ahead of the
//! commit that references them (`SPEC/19_large_values.md`).

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use jazz::db::{
    CellDelta, Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, RemoteLinkHint,
    SubscriptionStream, SyncCell, SyncMessage, Transport, TransportError, WireDelta, WireError,
    WireKind, WireOp, WireTarget, block_on, encode_frames,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::ReadViewSourceSpec;
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, Operation, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

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
    // Readable as its note is: its read policy reads `notes`, and through
    // the note's, `docs`.
    let comments = TableSchemaBuilder::new("comments")
        .fk_column("note", "notes")
        .policies(allow_all_policies().with_select(PolicyExpr::Inherits {
            operation: Operation::Select,
            via_column: "note".into(),
            max_depth: None,
        }));
    // Reads that include their doc read `docs`.
    let links = TableSchemaBuilder::new("links")
        .fk_column("doc", "docs")
        .policies(allow_all_policies());
    // Writable by anyone, readable by no session.
    let drafts = TableSchemaBuilder::new("drafts")
        .column("body", ColumnType::Text)
        .policies(allow_all_policies().with_select(PolicyExpr::False));
    let config = DbConfig::new(
        compile_schema(
            &SchemaBuilder::new()
                .table(docs)
                .table(titled)
                .table(notes)
                .table(comments)
                .table(links)
                .table(drafts)
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
    connect_wires(server, client, identity).0
}

type Wire = Rc<RefCell<VecDeque<SyncMessage>>>;

/// [`connect`], also handing back the wire up to the server and the wire
/// down from it, to inject or inspect messages.
fn connect_wires(server: &Db, client: &Db, identity: AuthorSubject) -> (Log, Wire, Wire) {
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
            out: down.clone(),
            inbound: up.clone(),
            log: log.clone(),
        }),
        identity,
    );
    (log, up, down)
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

/// A duplex that keeps the encoded size of each commit and `Rows` it
/// carries, and whether its chunked values crossed as deltas.
struct Spy {
    out: Wire,
    inbound: Wire,
    sizes: Rc<RefCell<Vec<(&'static str, usize, bool)>>>,
}

impl Transport for Spy {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let entry = match &message {
            SyncMessage::Commit { deltas, .. } => Some(("commit", !deltas.is_empty())),
            SyncMessage::Rows { images } => {
                let mut cells = images.iter().flat_map(|image| image.cells.iter());
                let large = cells
                    .clone()
                    .any(|(_, cell)| matches!(cell, SyncCell::Large(_) | SyncCell::Delta(_)));
                let delta = cells.any(|(_, cell)| matches!(cell, SyncCell::Delta(_)));
                large.then_some(("rows", delta))
            }
            _ => None,
        };
        if let Some((kind, delta)) = entry {
            let size = encode_frames(message.clone())
                .unwrap()
                .iter()
                .map(Vec::len)
                .sum();
            self.sizes.borrow_mut().push((kind, size, delta));
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

type Sizes = Rc<RefCell<Vec<(&'static str, usize, bool)>>>;

fn connect_spied(server: &Db, client: &Db, identity: AuthorSubject) -> (Sizes, Sizes) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let (sent, received) = (Sizes::default(), Sizes::default());
    let _ = block_on(client.connect_upstream(Box::new(Spy {
        out: up.clone(),
        inbound: down.clone(),
        sizes: sent.clone(),
    })));
    server.accept_subscriber(
        Box::new(Spy {
            out: down,
            inbound: up,
            sizes: received.clone(),
        }),
        identity,
    );
    (sent, received)
}

#[test]
fn edits_cross_the_wire_as_their_own_bytes() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let bob = open(2, user(2));
    let (alice_up, _) = connect_spied(&server, &alice, user(1));
    let (_, bob_down) = connect_spied(&server, &bob, user(2));
    let _bob_stream = subscribe(&bob, Query::from("docs"));
    let _alice_stream = subscribe(&alice, Query::from("docs"));
    let mut expected = text(4, LARGE);
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
    // Each append extends the epoch the last one crossed in: it travels as
    // its few bytes, not as the value's whole edit list.
    for n in 0..200 {
        let suffix = format!("<{n}>");
        block_on(alice.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
        expected.push_str(&suffix);
        pump(&server, &[&alice, &bob]);
    }
    for (wire, kind) in [(&alice_up, "commit"), (&bob_down, "rows")] {
        let wire = wire.borrow();
        let later: Vec<_> = wire.iter().filter(|(k, ..)| *k == kind).skip(2).collect();
        assert!(later.len() >= 150, "{kind}: {wire:?}");
        let deltas = later.iter().filter(|(_, _, delta)| *delta).count();
        assert!(
            deltas * 10 >= later.len() * 9,
            "{kind}: mostly deltas: {wire:?}"
        );
        let small = later.iter().filter(|(_, size, _)| *size < 512).count();
        assert!(small * 10 >= later.len() * 9, "{kind}: small: {wire:?}");
    }
    for db in [&server, &alice, &bob] {
        assert_eq!(
            local_cell(db, "docs", 1, "body").unwrap(),
            Some(Value::String(expected.clone()))
        );
    }
}

#[test]
fn writers_get_each_others_edits_as_deltas_over_their_own() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let bob = open(2, user(2));
    let (_, alice_down) = connect_spied(&server, &alice, user(1));
    let (_, bob_down) = connect_spied(&server, &bob, user(2));
    let _alice_stream = subscribe(&alice, Query::from("docs"));
    let _bob_stream = subscribe(&bob, Query::from("docs"));
    let mut expected = text(5, LARGE);
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
    // Each writer's own versions settle by its fate, unshipped; the
    // other's arrive as deltas over them.
    for n in 0..60 {
        let (writer, mark) = if n % 2 == 0 {
            (&alice, "a")
        } else {
            (&bob, "b")
        };
        let suffix = format!("<{mark}{n}>");
        block_on(writer.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
        expected.push_str(&suffix);
        pump(&server, &[&alice, &bob]);
    }
    for wire in [&alice_down, &bob_down] {
        let wire = wire.borrow();
        let later: Vec<_> = wire.iter().filter(|(k, ..)| *k == "rows").skip(2).collect();
        assert!(later.len() >= 25, "{wire:?}");
        let deltas = later.iter().filter(|(_, _, delta)| *delta).count();
        assert!(deltas * 10 >= later.len() * 9, "mostly deltas: {wire:?}");
    }
    for db in [&server, &alice, &bob] {
        assert_eq!(
            local_cell(db, "docs", 1, "body").unwrap(),
            Some(Value::String(expected.clone()))
        );
    }
}

/// A server-side duplex that names, in its first delta, a version the
/// replica does not hold (as after it evicted the row unannounced,
/// INV-SYNC-14), and logs the `Hold`s it receives.
struct Spoil {
    out: Wire,
    inbound: Wire,
    spoiled: Rc<RefCell<bool>>,
    holds: Rc<RefCell<Vec<Vec<u64>>>>,
}

impl Transport for Spoil {
    fn send(&mut self, mut message: SyncMessage) -> Result<(), TransportError> {
        if let SyncMessage::Rows { images } = &mut message
            && !*self.spoiled.borrow()
        {
            for (_, cell) in images.iter_mut().flat_map(|image| image.cells.iter_mut()) {
                if let SyncCell::Delta(delta) = cell {
                    delta.seq += 1 << 40;
                    *self.spoiled.borrow_mut() = true;
                }
            }
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let message = self.inbound.borrow_mut().pop_front()?;
        if let SyncMessage::Hold { rows } = &message {
            self.holds
                .borrow_mut()
                .push(rows.iter().map(|row| row.3).collect());
        }
        Some(message)
    }
}

#[test]
fn a_delta_over_a_version_the_replica_lacks_heals_by_asking_for_the_row() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let bob = open(2, user(2));
    connect(&server, &alice, user(1));
    let (up, down) = (Wire::default(), Wire::default());
    let spoiled = Rc::new(RefCell::new(false));
    let holds = Rc::new(RefCell::new(Vec::new()));
    let _ = block_on(bob.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
        log: Log::default(),
    })));
    server.accept_subscriber(
        Box::new(Spoil {
            out: down,
            inbound: up,
            spoiled: spoiled.clone(),
            holds: holds.clone(),
        }),
        user(2),
    );
    let _bob_stream = subscribe(&bob, Query::from("docs"));
    let mut expected = text(6, LARGE);
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
    for n in 0..6 {
        let suffix = format!("<{n}>");
        block_on(alice.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
        expected.push_str(&suffix);
        pump(&server, &[&alice, &bob]);
    }
    assert!(*spoiled.borrow(), "a delta crossed");
    assert!(
        holds.borrow().iter().any(|seqs| seqs == &[0]),
        "bob asked for the row whole: {:?}",
        holds.borrow()
    );
    assert_eq!(
        local_cell(&bob, "docs", 1, "body").unwrap(),
        Some(Value::String(expected))
    );
}

#[test]
fn a_delta_over_a_version_the_writer_cannot_read_is_rejected() {
    let server = open(0, AuthorSubject::SYSTEM);
    let mallory = open(9, user(9));
    let (_, up, down) = connect_wires(&server, &mallory, user(9));
    insert(
        &server,
        "drafts",
        1,
        vec![("body", Value::String(text(7, LARGE)))],
    );
    pump(&server, &[&mallory]);
    let update = |tx: u64, body: Value, deltas: Vec<CellDelta>| SyncMessage::Commit {
        node: [9; 16],
        tx,
        kind: WireKind::Mergeable,
        ops: vec![WireOp::Update {
            table: "drafts".to_owned(),
            row: *row(1).0.as_bytes(),
            cells: [("body".to_owned(), body)].into(),
            target: WireTarget::Root,
            at: None,
        }],
        author: None,
        session: None,
        expect: Vec::new(),
        deltas,
        bases: Vec::new(),
        origin: None,
    };
    // Each version the server could have written the draft at, named as a
    // base: rebuilt, the write would carry the draft's hidden body.
    for seq in 1..=4u64 {
        up.borrow_mut().push_back(update(
            seq,
            Value::Nullable(None),
            vec![CellDelta {
                op: 0,
                column: "body".to_owned(),
                depth: 0,
                key: Vec::new(),
                delta: WireDelta {
                    seq,
                    base: 0,
                    edits: Vec::new(),
                },
            }],
        ));
    }
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    // Refused for the base, before the update's own policies are checked.
    let fates: Vec<(u64, bool)> = down
        .borrow()
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Fate {
                tx,
                result: Err(WireError::Rejected { message, .. }),
                ..
            } => Some((*tx, message.contains("large-value delta"))),
            SyncMessage::Fate { tx, .. } => Some((*tx, false)),
            _ => None,
        })
        .collect();
    assert_eq!(fates, vec![(1, true), (2, true), (3, true), (4, true)]);
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
fn held_upload() -> (Db, Db, (Log, Wire, Wire)) {
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
    let wires = connect_wires(&server, &alice, user(1));
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
    (server, alice, wires)
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

/// A read that reaches a table the held commits write only through its
/// policy (directly, or inherited through another table's) or through an
/// include waits too: the held write could change what it sees.
#[test]
fn reads_reaching_a_held_table_through_policies_or_includes_wait() {
    for (what, query) in [
        ("a policy reading docs", Query::from("notes")),
        (
            "a policy inherited from one reading docs",
            Query::from("comments"),
        ),
        ("an include of docs", Query::from("links").include("doc")),
    ] {
        let (server, alice, _wires) = held_upload();
        let mut stream = block_on(alice.subscribe(
            &alice.prepare_query(&query).unwrap(),
            ReadOpts {
                tier: ReadTier::Remote,
                ..ReadOpts::default()
            },
        ))
        .unwrap();
        let mut shown = BTreeSet::new();
        pump(&server, &[&alice]);
        assert!(
            !drain(&mut stream, &mut shown),
            "a read through {what} waits behind the held commit"
        );
        server.set_large_value_staging_policy(jazz::db::LargeValueStagingPolicy {
            incoming_bytes_per_window: u64::MAX,
            window_ms: 60_000,
            max_age_ms: 10 * 60 * 1_000,
        });
        pump(&server, &[&alice]);
        assert!(
            drain(&mut stream, &mut shown),
            "a read through {what} is answered once the upload lands"
        );
    }
    // The control: the same tables without the include, the policy-free
    // `links`, depend on nothing held and are served at once.
    let (server, alice, _wires) = held_upload();
    let mut stream = remote_subscribe(&alice, "links");
    pump(&server, &[&alice]);
    assert!(
        drain(&mut stream, &mut BTreeSet::new()),
        "a read of links alone overtakes"
    );
}

/// A read frozen at a snapshot that names the writer's held commit waits
/// for it, though its table is another: the server cannot resolve the
/// snapshot before that commit applies, and would refuse it.
#[test]
fn a_snapshot_read_naming_a_held_commit_waits_for_it() {
    let (server, alice, (_log, up, down)) = held_upload();
    // Captured offline, the reference names the unsettled upload.
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let snapshot = block_on(alice.capture_snapshot()).unwrap();
    assert!(
        !snapshot.dots.is_empty(),
        "the capture names the held commit"
    );
    // Alice's own store answers that read itself, so the read goes up as a
    // relay would send it: on the wire, behind the held commit.
    const READ: u64 = 9_999;
    up.borrow_mut().push_back(SyncMessage::Fetch {
        id: READ,
        query: Query::from("titled"),
        binding: BTreeMap::new(),
        at: None,
        include_deleted: false,
        tier: ReadTier::Remote,
        wait: None,
        view: ReadViewSourceSpec::Snapshot { snapshot },
    });
    let reply = || {
        down.borrow().iter().find_map(|message| match message {
            SyncMessage::Answer { id: READ, .. } => Some(Ok(())),
            SyncMessage::Refused {
                id: READ, message, ..
            } => Some(Err(message.clone())),
            _ => None,
        })
    };
    for _ in 0..3 {
        block_on(server.tick()).unwrap();
    }
    assert_eq!(reply(), None, "the read waits behind the held commit");

    server.set_large_value_staging_policy(jazz::db::LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 10 * 60 * 1_000,
    });
    for _ in 0..3 {
        block_on(server.tick()).unwrap();
    }
    assert!(local_cell(&server, "docs", 2, "body").unwrap().is_some());
    assert_eq!(reply(), Some(Ok(())), "then the snapshot resolves");
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

//! Chunked large values over sync: row images carry descriptors, replicas
//! pull chunks (prefetching indexed columns before the row is
//! installed), and clients push the nodes of their new values ahead of the
//! commit that references them (`SPEC/19_large_values.md`).

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use jazz::db::{
    AccessProof, Acting, BuiltOn, CellDelta, Db, DbConfig, DbIdentity, InsertOptions, Outcome,
    ReadOpts, ReadSpan, ReadTier, RemoteLinkHint, SubscriptionStream, SyncCell, SyncMessage,
    Transport, TransportError, WireBase, WireChunk, WireDelta, WireError, WireKind, WireOp,
    WirePull, WireTarget, WireWrite, block_on, encode_frames,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::ReadViewSourceSpec;
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{
    CmpOp, ColumnMergeStrategy, ColumnType, Operation, PolicyExpr, PolicyValue, SchemaBuilder,
    TableName, TableSchemaBuilder,
};

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
    // Readable while `public`: a reader may see some of a row's versions
    // and not others.
    let shared = TableSchemaBuilder::new("shared")
        .column("body", ColumnType::Text)
        .column("public", ColumnType::Boolean)
        .policies(allow_all_policies().with_select(PolicyExpr::Cmp {
            column: "public".to_owned(),
            op: CmpOp::Eq,
            value: PolicyValue::Literal(jazz::tools::Value::Boolean(true)),
        }));
    // Its `body` merges concurrent text edits (SPEC 4 §4.3).
    let merged = TableSchemaBuilder::new("merged")
        .column("body", ColumnType::Text)
        .policies(allow_all_policies());
    let mut schema = SchemaBuilder::new()
        .table(docs)
        .table(titled)
        .table(notes)
        .table(comments)
        .table(links)
        .table(drafts)
        .table(merged)
        .table(shared)
        .build();
    for column in &mut schema
        .get_mut(&TableName::new("merged"))
        .unwrap()
        .columns
        .columns
    {
        *column = column.clone().merge_strategy(ColumnMergeStrategy::Text);
    }
    let config = DbConfig::new(
        compile_schema(&schema),
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
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down.clone(),
                inbound: up.clone(),
                log: log.clone(),
            }),
            identity,
        )
        .unwrap();
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

/// One commit holding the same large value in two rows proves the pushed
/// tree for both: the receipts settle only once the whole commit applies.
#[test]
fn one_commit_may_hold_the_same_large_value_twice() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    connect(&server, &alice, user(1));
    let body = text(41, LARGE);
    block_on(alice.transaction(async |tx| {
        for n in [1, 2] {
            tx.insert(
                "docs",
                BTreeMap::from([
                    ("body".to_owned(), Value::String(body.clone())),
                    ("blob".to_owned(), Value::Bytes(Vec::new())),
                ]),
                InsertOptions {
                    row_id: Some(row(n)),
                    ..Default::default()
                },
            )
            .await?;
        }
        Ok(())
    }))
    .unwrap();
    pump(&server, &[&alice]);
    for n in [1, 2] {
        assert_eq!(
            local_cell(&server, "docs", n, "body").unwrap(),
            Some(Value::String(body.clone()))
        );
    }
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
fn a_range_read_pulls_its_whole_path_in_one_round_trip() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let log = connect(&server, &alice, user(1));
    let blob: Vec<u8> = (0..LARGE * 16).map(|n| (n * 7 % 251) as u8).collect();
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String("small".into())),
            ("blob", Value::Bytes(blob.clone())),
        ],
    );
    let _stream = subscribe(&alice, Query::from("docs").select(["body"]));
    pump(&server, &[&alice]);
    log.borrow_mut().clear();
    let range = 3_000_000..3_000_100;
    let first = block_on(alice.read_value_range("docs", row(1), "blob", range.clone()));
    assert!(first.is_err(), "the first read asks for the range");
    // One pull up, one answer down: the whole path, not a level per trip.
    block_on(alice.tick()).unwrap();
    block_on(server.tick()).unwrap();
    block_on(alice.tick()).unwrap();
    let got = block_on(alice.read_value_range("docs", row(1), "blob", range.clone()));
    assert_eq!(got.unwrap(), blob[3_000_000..3_000_100]);
    let pulls = log
        .borrow()
        .iter()
        .filter(|kind| **kind == "requests")
        .count();
    assert_eq!(pulls, 1, "{:?}", log.borrow());
    let (after, _) = alice.large_value_nodes_for_test().unwrap();
    let (total, _) = server.large_value_nodes_for_test().unwrap();
    assert!(after < total, "only the covering path: {after} of {total}");
}

#[test]
fn a_utf16_range_read_pulls_its_whole_path_in_one_round_trip() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let log = connect(&server, &alice, user(1));
    // Mixed widths: one, two and three UTF-8 bytes per code unit.
    let body: String = text(5, LARGE * 4)
        .chars()
        .enumerate()
        .map(|(n, c)| match n % 3 {
            0 => c,
            1 => 'é',
            _ => '€',
        })
        .collect();
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String(body.clone())),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    let _stream = subscribe(&alice, Query::from("docs").select(["blob"]));
    pump(&server, &[&alice]);
    log.borrow_mut().clear();
    let range = 700_000..700_050;
    let first = block_on(alice.read_text_utf16_range("docs", row(1), "body", range.clone()));
    assert!(first.is_err(), "the first read asks for the range");
    block_on(alice.tick()).unwrap();
    block_on(server.tick()).unwrap();
    block_on(alice.tick()).unwrap();
    let got = block_on(alice.read_text_utf16_range("docs", row(1), "body", range.clone()));
    let expected: String = body.chars().skip(700_000).take(50).collect();
    assert_eq!(got.unwrap(), expected);
    assert_eq!(requests(&log), 1, "{:?}", log.borrow());
    let (after, _) = alice.large_value_nodes_for_test().unwrap();
    let (total, _) = server.large_value_nodes_for_test().unwrap();
    assert!(after < total, "only the covering paths: {after} of {total}");
}

#[test]
fn pulls_answer_only_for_versions_the_reader_may_read() {
    let server = open(0, AuthorSubject::SYSTEM);
    let mallory = open(9, user(9));
    let (_, up, down) = connect_wires(&server, &mallory, user(9));
    let _stream = subscribe(&mallory, Query::from("docs").select(["blob"]));
    pump(&server, &[&mallory]);
    insert(
        &server,
        "drafts",
        2,
        vec![("body", Value::String(text(8, LARGE)))],
    );
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String(text(1, LARGE))),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    let draft = jazz::large_values::prepare(
        jazz::large_values::LargeValueKind::String,
        text(8, LARGE).as_bytes(),
    )
    .unwrap()
    .value_ref
    .root
    .object_hash
    .0;
    // A node this store never held.
    let unknown = jazz::large_values::prepare(
        jazz::large_values::LargeValueKind::String,
        text(9, LARGE).as_bytes(),
    )
    .unwrap()
    .value_ref
    .root
    .object_hash
    .0;
    // The doc's version as shipped to Mallory, read off the wire before
    // her replica takes it.
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let (seq, key, doc) = down
        .borrow()
        .iter()
        .find_map(|message| match message {
            SyncMessage::Rows { images, .. } => images.iter().find_map(|image| {
                let cell = image.cells.iter().find_map(|(name, cell)| match cell {
                    SyncCell::Large(descriptor) if name.ends_with("body") => Some(descriptor),
                    _ => None,
                })?;
                let value = jazz::large_values::decode_large_value_ref(cell).ok()?;
                Some((image.seq, image.key.clone(), value.root.object_hash.0))
            }),
            _ => None,
        })
        .expect("the doc's image");
    down.borrow_mut().clear();
    let pull = |table: &str, id: u8, seq: u64, root: [u8; 32], nodes: Vec<[u8; 32]>| WirePull {
        table: table.to_owned(),
        id: *row(id).0.as_bytes(),
        key: key.clone(),
        seq,
        root,
        nodes,
        span: Some((0, LARGE as u64)),
    };
    let mut pulls = vec![
        // The doc's own tree, through its version: answered.
        pull("docs", 1, seq, doc, vec![doc]),
        // The draft's tree through the doc's version, as its root or as
        // a node under the doc's: neither is that version's.
        pull("docs", 1, seq, draft, vec![draft]),
        pull("docs", 1, seq, doc, vec![draft]),
        pull("docs", 1, seq, doc, vec![unknown]),
    ];
    // Through the draft's own row, at any version: she may read none.
    pulls.extend((0..seq + 4).map(|seq| pull("drafts", 2, seq, draft, vec![draft])));
    up.borrow_mut()
        .push_back(SyncMessage::ChunkRequests { pulls });
    block_on(server.tick()).unwrap();
    let mut sent = BTreeSet::new();
    let mut missing = BTreeSet::new();
    for message in down.borrow().iter() {
        if let SyncMessage::Chunks {
            nodes,
            missing: absent,
        } = message
        {
            sent.extend(nodes.iter().map(|node| node.hash));
            missing.extend(absent.iter().map(|node| node.hash));
        }
    }
    assert!(sent.contains(&doc), "the doc's tree is answered");
    assert!(sent.len() > 1, "with the nodes beneath it the span covers");
    assert!(!sent.contains(&draft), "the draft's tree never is");
    // Held for a row she cannot read, or never held: answered alike.
    assert!(missing.contains(&draft));
    assert!(missing.contains(&unknown));
}

/// A replica orders synced chunked values as the authority does: by cell,
/// before every inline value (SPEC 19.5), with no chunk read.
#[test]
fn synced_chunked_values_sort_before_inline_ones() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let _log = connect(&server, &alice, user(1));
    let base = text(2, LARGE);
    let titles = ["short".to_owned(), format!("{base}b"), format!("{base}a")];
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
    let query = Query::from("titled")
        .select(["id"])
        .order_by("title", OrderDirection::Asc);
    for db in [&server, &alice] {
        let rows = block_on(db.all(
            &db.prepare_query(&query).unwrap(),
            ReadOpts {
                tier: ReadTier::LocalOnly,
                ..ReadOpts::default()
            },
        ))
        .unwrap();
        let ids: Vec<RowUuid> = rows.iter().map(|r| r.row_uuid()).collect();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[2], row(1), "the inline title sorts last");
    }
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
            SyncMessage::Commit { ops, .. } => {
                Some(("commit", ops.iter().any(|op| !op.deltas.is_empty())))
            }
            SyncMessage::PushChunks { .. } => Some(("push", false)),
            SyncMessage::Rows { images, .. } => {
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
    server
        .accept_subscriber(
            Box::new(Spy {
                out: down,
                inbound: up,
                sizes: received.clone(),
            }),
            identity,
        )
        .unwrap();
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
/// INV-SYNC-14), and logs the `ResendWhole`s it receives.
struct Spoil {
    out: Wire,
    inbound: Wire,
    spoiled: Rc<RefCell<bool>>,
    holds: Rc<RefCell<Vec<Vec<u64>>>>,
    /// The `ResendWhole`s are lost on the way, as in a link that drops.
    lose_holds: bool,
}

impl Transport for Spoil {
    fn send(&mut self, mut message: SyncMessage) -> Result<(), TransportError> {
        if let SyncMessage::Rows { images, .. } = &mut message
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
        if let SyncMessage::ResendWhole { rows } = &message {
            self.holds
                .borrow_mut()
                .push(rows.iter().map(|row| row.3).collect());
            if self.lose_holds {
                return self.try_recv();
            }
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
    server
        .accept_subscriber(
            Box::new(Spoil {
                out: down,
                inbound: up,
                spoiled: spoiled.clone(),
                holds: holds.clone(),
                lose_holds: false,
            }),
            user(2),
        )
        .unwrap();
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

/// A replica whose `ResendWhole` is lost (the link drops with it in flight)
/// holds a stale version that no resume may count as held: the results it
/// settled meanwhile are not stored as resume points, and the row is asked
/// for again on the next connection.
#[test]
fn a_row_asked_for_whole_heals_after_its_hold_is_lost() {
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
    server
        .accept_subscriber(
            Box::new(Spoil {
                out: down,
                inbound: up,
                spoiled: spoiled.clone(),
                holds: holds.clone(),
                lose_holds: true,
            }),
            user(2),
        )
        .unwrap();
    let bob_stream = subscribe(&bob, Query::from("docs"));
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
    let suffix = "<lost>".to_owned();
    block_on(alice.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
    expected.push_str(&suffix);
    pump(&server, &[&alice, &bob]);
    assert!(*spoiled.borrow(), "a delta crossed");
    assert!(!holds.borrow().is_empty(), "bob asked for the row whole");
    assert_ne!(
        local_cell(&bob, "docs", 1, "body").unwrap(),
        Some(Value::String(expected.clone())),
        "the lost ask leaves bob stale"
    );
    // The link drops; bob resubscribes on a new one.
    drop(bob_stream);
    let _ = connect(&server, &bob, user(2));
    let _bob_stream = subscribe(&bob, Query::from("docs"));
    pump(&server, &[&alice, &bob]);
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
        ops: vec![WireOp {
            deltas,
            ..WireWrite::Update {
                table: "drafts".to_owned(),
                row: *row(1).0.as_bytes(),
                cells: [("body".to_owned(), body)].into(),
                target: WireTarget::Root,
                at: None,
            }
            .into()
        }],
        acting: Acting::Own,
        origin: None,
    };
    // Each version the server could have written the draft at, named as a
    // base: rebuilt, the write would carry the draft's hidden body.
    for seq in 1..=4u64 {
        up.borrow_mut().push_back(update(
            seq,
            Value::Nullable(None),
            vec![CellDelta {
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
                outcome:
                    Outcome::Refused {
                        error: WireError::Rejected { message, .. },
                        ..
                    },
                ..
            } => Some((*tx, message.contains("large-value delta"))),
            SyncMessage::Fate { tx, .. } => Some((*tx, false)),
            _ => None,
        })
        .collect();
    assert_eq!(fates, vec![(1, true), (2, true), (3, true), (4, true)]);
}

/// A pull that cites a version the reader could read then, after the reader
/// lost access to the row, is answered with nothing (SPEC 19.9, INV-RLS-13).
#[test]
fn pulls_after_read_access_is_lost_are_answered_with_nothing() {
    let server = open(0, AuthorSubject::SYSTEM);
    let mallory = open(9, user(9));
    let (_, up, down) = connect_wires(&server, &mallory, user(9));
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String("x".to_owned())),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    let _stream = subscribe(&mallory, Query::from("notes"));
    pump(&server, &[&mallory]);
    insert(
        &server,
        "notes",
        5,
        vec![("text", Value::String(text(3, LARGE)))],
    );
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let (seq, key, root) = down
        .borrow()
        .iter()
        .find_map(|message| match message {
            SyncMessage::Rows { images, .. } => images.iter().find_map(|image| {
                let cell = image.cells.iter().find_map(|(name, cell)| match cell {
                    SyncCell::Large(descriptor) if name.ends_with("text") => Some(descriptor),
                    _ => None,
                })?;
                let value = jazz::large_values::decode_large_value_ref(cell).ok()?;
                Some((image.seq, image.key.clone(), value.root.object_hash.0))
            }),
            _ => None,
        })
        .expect("the note's image");
    // Access goes: no doc exists any more.
    block_on(server.delete("docs", row(1), Default::default())).unwrap();
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let evicted = down
        .borrow()
        .iter()
        .any(|message| matches!(message, SyncMessage::AccessLost { .. }));
    down.borrow_mut().clear();
    up.borrow_mut().push_back(SyncMessage::ChunkRequests {
        pulls: vec![WirePull {
            table: "notes".to_owned(),
            id: *row(5).0.as_bytes(),
            key,
            seq,
            root,
            nodes: vec![root],
            span: Some((0, LARGE as u64)),
        }],
    });
    block_on(server.tick()).unwrap();
    let mut sent = BTreeSet::new();
    for message in down.borrow().iter() {
        if let SyncMessage::Chunks { nodes, .. } = message {
            sent.extend(nodes.iter().map(|node| node.hash));
        }
    }
    eprintln!("REVIEW PROBE: evicted={evicted} sent_nodes={}", sent.len());
    assert!(
        sent.is_empty(),
        "nodes of a row the reader can no longer read were sent ({} nodes, evicted={evicted})",
        sent.len()
    );
}

/// One `ChunkRequests` is answered with a bounded number of bytes, each
/// node once; the pulls past the budget are answered once that has been
/// sent, so a frame of many pulls cannot grow the outbox without bound.
#[test]
fn an_answer_to_many_pulls_is_bounded_and_the_rest_follows() {
    const MB: u64 = 1 << 20;
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let (_, up, down) = connect_wires(&server, &alice, user(1));
    let _stream = subscribe(&alice, Query::from("docs"));
    pump(&server, &[&alice]);
    let blob: Vec<u8> = text(9, 32 * MB as usize).into_bytes();
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String("x".to_owned())),
            ("blob", Value::Bytes(blob)),
        ],
    );
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let (seq, key, root) = down
        .borrow()
        .iter()
        .find_map(|message| match message {
            SyncMessage::Rows { images, .. } => images.iter().find_map(|image| {
                let cell = image.cells.iter().find_map(|(name, cell)| match cell {
                    SyncCell::Large(descriptor) if name.ends_with("blob") => Some(descriptor),
                    _ => None,
                })?;
                let value = jazz::large_values::decode_large_value_ref(cell).ok()?;
                Some((image.seq, image.key.clone(), value.root.object_hash.0))
            }),
            _ => None,
        })
        .expect("the doc's image");
    down.borrow_mut().clear();
    // Eight 4 MiB spans, each named twice.
    let pull = |at: u64| WirePull {
        table: "docs".to_owned(),
        id: *row(1).0.as_bytes(),
        key: key.clone(),
        seq,
        root,
        nodes: vec![root],
        span: Some((at * 4 * MB, (at + 1) * 4 * MB)),
    };
    up.borrow_mut().push_back(SyncMessage::ChunkRequests {
        pulls: (0..8).chain(0..8).map(pull).collect(),
    });
    let answered = |down: &Wire| {
        let mut bytes = 0;
        let mut hashes = Vec::new();
        for message in down.borrow_mut().drain(..) {
            if let SyncMessage::Chunks { nodes, .. } = message {
                for node in nodes {
                    bytes += node.bytes.len() as u64;
                    hashes.push(node.hash);
                }
            }
        }
        (bytes, hashes)
    };
    block_on(server.tick()).unwrap();
    let (first, hashes) = answered(&down);
    assert!(
        first > 0 && first <= 17 * MB,
        "the first answer carries {first} bytes"
    );
    let once: BTreeSet<_> = hashes.iter().collect();
    assert_eq!(once.len(), hashes.len(), "an answer sends each node once");
    // The deferred pulls are answered over the next ticks: every span's
    // nodes arrive.
    let mut all: BTreeSet<[u8; 32]> = hashes.into_iter().collect();
    for _ in 0..8 {
        block_on(server.tick()).unwrap();
        all.extend(answered(&down).1);
    }
    let mut whole = BTreeSet::new();
    up.borrow_mut().push_back(SyncMessage::ChunkRequests {
        pulls: vec![pull(0)],
    });
    for _ in 0..8 {
        block_on(server.tick()).unwrap();
        whole.extend(answered(&down).1);
    }
    assert!(whole.is_subset(&all));
    assert!(all.len() > 4 * whole.len(), "all eight spans were answered");
}

#[test]
fn naming_a_hidden_trees_hash_reads_nothing() {
    let server = open(0, AuthorSubject::SYSTEM);
    let mallory = open(9, user(9));
    let (_, up, down) = connect_wires(&server, &mallory, user(9));
    insert(
        &server,
        "drafts",
        1,
        vec![("body", Value::String(text(8, LARGE)))],
    );
    let hidden = jazz::large_values::prepare(
        jazz::large_values::LargeValueKind::String,
        text(8, LARGE).as_bytes(),
    )
    .unwrap()
    .value_ref;
    let unknown = jazz::large_values::prepare(
        jazz::large_values::LargeValueKind::String,
        text(9, LARGE).as_bytes(),
    )
    .unwrap()
    .value_ref;
    pump(&server, &[&mallory]);
    // Mallory names the draft's tree (its hashes are no secret) in a doc
    // she may write and read, pushing nothing; and a tree nobody stored.
    let insert = |tx: u64, n: u8, value: &jazz::large_values::LargeValueRef| SyncMessage::Commit {
        node: [9; 16],
        tx,
        kind: WireKind::Mergeable,
        ops: vec![
            WireWrite::Insert {
                table: "docs".to_owned(),
                row: *row(n).0.as_bytes(),
                cells: [
                    ("body".to_owned(), Value::Large(Box::new(value.clone()))),
                    ("blob".to_owned(), Value::Bytes(Vec::new())),
                ]
                .into(),
                target: WireTarget::Root,
                at: None,
            }
            .into(),
        ],
        acting: Acting::Own,
        origin: None,
    };
    up.borrow_mut().push_back(insert(1, 1, &hidden));
    up.borrow_mut().push_back(insert(2, 2, &unknown));
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let fates: Vec<(u64, String)> = down
        .borrow()
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Fate {
                tx,
                outcome:
                    Outcome::Refused {
                        error: WireError::Rejected { message, .. },
                        ..
                    },
                ..
            } => Some((*tx, message.clone())),
            SyncMessage::Fate { tx, .. } => Some((*tx, "accepted".to_owned())),
            _ => None,
        })
        .collect();
    assert_eq!(fates.len(), 2, "{fates:?}");
    // Refused alike: the answer does not tell what the server holds.
    assert_eq!(fates[0].1, fates[1].1, "{fates:?}");
    assert!(fates[0].1.contains("does not hold"), "{fates:?}");
    assert_eq!(local_cell(&server, "docs", 1, "body").unwrap(), None);
}

/// An access proof names a version of the written row, and proves its trees
/// only while the writer may read that version: naming a version the writer
/// may not read, of a row it may read now, proves nothing, and is refused
/// exactly as a tree nobody stored is (SPEC 19.4, "What possession
/// prevents").
#[test]
fn an_access_proof_the_writer_cannot_read_proves_nothing() {
    let server = open(0, AuthorSubject::SYSTEM);
    let mallory = open(9, user(9));
    let (_, up, down) = connect_wires(&server, &mallory, user(9));
    // The row held a secret body while private, then went public with
    // another.
    insert(
        &server,
        "shared",
        1,
        vec![
            ("body", Value::String(text(8, LARGE))),
            ("public", Value::Bool(false)),
        ],
    );
    let secret = server.write_conflicts("shared", row(1), None).unwrap()[0].seq;
    block_on(
        server.update(
            "shared",
            row(1),
            [
                ("body".to_owned(), Value::String("public".into())),
                ("public".to_owned(), Value::Bool(true)),
            ]
            .into(),
            Default::default(),
        ),
    )
    .unwrap();
    let hidden = jazz::large_values::prepare(
        jazz::large_values::LargeValueKind::String,
        text(8, LARGE).as_bytes(),
    )
    .unwrap()
    .value_ref;
    let unknown = jazz::large_values::prepare(
        jazz::large_values::LargeValueKind::String,
        text(9, LARGE).as_bytes(),
    )
    .unwrap()
    .value_ref;
    pump(&server, &[&mallory]);
    // Mallory writes the secret body's tree back into the row, naming the
    // private version as her proof; then a tree nobody stored.
    let update = |tx: u64, value: &jazz::large_values::LargeValueRef| SyncMessage::Commit {
        node: [9; 16],
        tx,
        kind: WireKind::Mergeable,
        ops: vec![WireOp {
            access_proof: Some(AccessProof {
                key: Vec::new(),
                seq: secret,
            }),
            ..WireWrite::Update {
                table: "shared".to_owned(),
                row: *row(1).0.as_bytes(),
                cells: [("body".to_owned(), Value::Large(Box::new(value.clone())))].into(),
                target: WireTarget::Root,
                at: None,
            }
            .into()
        }],
        acting: Acting::Own,
        origin: None,
    };
    up.borrow_mut().push_back(update(1, &hidden));
    up.borrow_mut().push_back(update(2, &unknown));
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let fates: Vec<String> = down
        .borrow()
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Fate {
                outcome:
                    Outcome::Refused {
                        error: WireError::Rejected { message, .. },
                        ..
                    },
                ..
            } => Some(message.clone()),
            SyncMessage::Fate { .. } => Some("accepted".to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(fates.len(), 2, "{fates:?}");
    assert_eq!(fates[0], fates[1], "{fates:?}");
    assert!(fates[0].contains("does not hold"), "{fates:?}");
    assert_eq!(
        local_cell(&server, "shared", 1, "body").unwrap(),
        Some(Value::String("public".into()))
    );
}

/// Pushed nodes are taken silently: the server answers a push of nodes it
/// already holds (for a row the pusher cannot read) as it answers a push of
/// new ones, with nothing (SPEC 19.9, "No existence oracle"). A node held
/// for someone else proves nothing until the writer pushed it itself
/// (SPEC 19.4, "What possession prevents").
#[test]
fn a_push_is_never_answered() {
    let server = open(0, AuthorSubject::SYSTEM);
    let mallory = open(9, user(9));
    let (_, up, down) = connect_wires(&server, &mallory, user(9));
    insert(
        &server,
        "drafts",
        1,
        vec![("body", Value::String(text(8, LARGE)))],
    );
    pump(&server, &[&mallory]);
    let prepared = |seed| {
        jazz::large_values::prepare(
            jazz::large_values::LargeValueKind::String,
            text(seed, LARGE).as_bytes(),
        )
        .unwrap()
    };
    let hidden = prepared(8).value_ref;
    let insert = |tx: u64, n: u8| SyncMessage::Commit {
        node: [9; 16],
        tx,
        kind: WireKind::Mergeable,
        ops: vec![
            WireWrite::Insert {
                table: "docs".to_owned(),
                row: *row(n).0.as_bytes(),
                cells: [
                    ("body".to_owned(), Value::Large(Box::new(hidden.clone()))),
                    ("blob".to_owned(), Value::Bytes(Vec::new())),
                ]
                .into(),
                target: WireTarget::Root,
                at: None,
            }
            .into(),
        ],
        acting: Acting::Own,
        origin: None,
    };
    let fates = || -> Vec<bool> {
        down.borrow()
            .iter()
            .filter_map(|message| match message {
                SyncMessage::Fate { outcome, .. } => Some(outcome.result().is_ok()),
                _ => None,
            })
            .collect()
    };
    // Held for the draft only: naming it proves nothing.
    up.borrow_mut().push_back(insert(1, 1));
    for _ in 0..2 {
        block_on(server.tick()).unwrap();
    }
    assert_eq!(fates(), [false]);
    let before = down.borrow().len();
    for seed in [8, 9] {
        up.borrow_mut().push_back(SyncMessage::PushChunks {
            nodes: prepared(seed)
                .staged_chunks
                .into_iter()
                .map(|chunk| WireChunk {
                    hash: chunk.node_ref.object_hash.0,
                    bytes: chunk.encoded,
                })
                .collect(),
        });
        for _ in 0..2 {
            block_on(server.tick()).unwrap();
        }
        assert_eq!(
            down.borrow().len(),
            before,
            "{:?}",
            down.borrow().iter().skip(before).collect::<Vec<_>>()
        );
    }
    // Pushed by Mallory herself, the same tree is hers to write: she holds
    // its content, as the push shows.
    up.borrow_mut().push_back(insert(2, 2));
    for _ in 0..2 {
        block_on(server.tick()).unwrap();
    }
    assert_eq!(fates(), [false, true]);
}

/// A delta stands in for its op's NULL cell, once per column: a second
/// delta for a column, or one over a cell holding a value, refuses the
/// commit rather than replacing what was sent (SPEC 8.2).
#[test]
fn a_delta_that_does_not_stand_in_for_a_null_cell_is_refused() {
    let server = open(0, AuthorSubject::SYSTEM);
    let mallory = open(9, user(9));
    let (_, up, down) = connect_wires(&server, &mallory, user(9));
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String(text(7, LARGE))),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    pump(&server, &[&mallory]);
    let delta = || CellDelta {
        column: "body".to_owned(),
        depth: 0,
        key: Vec::new(),
        delta: WireDelta {
            seq: 1,
            base: 0,
            edits: Vec::new(),
        },
    };
    let update = |tx: u64, body: Value, deltas: Vec<CellDelta>| SyncMessage::Commit {
        node: [9; 16],
        tx,
        kind: WireKind::Mergeable,
        ops: vec![WireOp {
            deltas,
            ..WireWrite::Update {
                table: "docs".to_owned(),
                row: *row(1).0.as_bytes(),
                cells: [("body".to_owned(), body)].into(),
                target: WireTarget::Root,
                at: None,
            }
            .into()
        }],
        acting: Acting::Own,
        origin: None,
    };
    up.borrow_mut()
        .push_back(update(1, Value::Nullable(None), vec![delta(), delta()]));
    up.borrow_mut()
        .push_back(update(2, Value::String("sent".into()), vec![delta()]));
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let fates: Vec<String> = down
        .borrow()
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Fate {
                outcome:
                    Outcome::Refused {
                        error: WireError::Rejected { message, .. },
                        ..
                    },
                ..
            } => Some(message.clone()),
            SyncMessage::Fate { .. } => Some("accepted".to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(fates.len(), 2, "{fates:?}");
    assert!(
        fates[0].contains("names a column another delta"),
        "{fates:?}"
    );
    assert!(fates[1].contains("stands in for no NULL cell"), "{fates:?}");
}

/// A text write the authority merges is checked like any other: its
/// descriptor naming a hidden tree is refused before the merge reads it,
/// so neither the merged value nor the write as sent takes in its text.
#[test]
fn a_merged_text_write_naming_a_hidden_tree_is_refused() {
    let server = open(0, AuthorSubject::SYSTEM);
    let mallory = open(9, user(9));
    let (_, up, down) = connect_wires(&server, &mallory, user(9));
    insert(
        &server,
        "drafts",
        1,
        vec![("body", Value::String(text(8, LARGE)))],
    );
    let hidden = jazz::large_values::prepare(
        jazz::large_values::LargeValueKind::String,
        text(8, LARGE).as_bytes(),
    )
    .unwrap()
    .value_ref;
    insert(
        &server,
        "merged",
        2,
        vec![("body", Value::String("hello".into()))],
    );
    let seed = server.write_conflicts("merged", row(2), None).unwrap()[0].seq;
    block_on(server.update(
        "merged",
        row(2),
        [("body".to_owned(), Value::String("hello there".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &[&mallory]);
    // Mallory edits the doc over its first version, which the authority
    // has to merge, naming the draft's tree as her value.
    up.borrow_mut().push_back(SyncMessage::Commit {
        node: [9; 16],
        tx: 1,
        kind: WireKind::Mergeable,
        ops: vec![WireOp {
            bases: vec![WireBase {
                key: Vec::new(),
                on: BuiltOn::Seq(seed),
            }],
            ..WireWrite::Update {
                table: "merged".to_owned(),
                row: *row(2).0.as_bytes(),
                cells: [("body".to_owned(), Value::Large(Box::new(hidden)))].into(),
                target: WireTarget::Root,
                at: None,
            }
            .into()
        }],
        acting: Acting::Own,
        origin: None,
    });
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let fates: Vec<String> = down
        .borrow()
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Fate {
                outcome:
                    Outcome::Refused {
                        error: WireError::Rejected { message, .. },
                        ..
                    },
                ..
            } => Some(message.clone()),
            SyncMessage::Fate { .. } => Some("accepted".to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(fates.len(), 1, "{fates:?}");
    assert!(fates[0].contains("does not hold"), "{fates:?}");
    assert_eq!(
        local_cell(&server, "merged", 2, "body").unwrap(),
        Some(Value::String("hello there".into()))
    );
    assert_eq!(server.as_sent_versions_for_test().unwrap(), 0);
}

#[test]
fn rebuilt_trees_push_only_their_new_nodes_even_while_commits_are_in_flight() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let bob = open(2, user(2));
    let (alice_up, _) = connect_spied(&server, &alice, user(1));
    connect(&server, &bob, user(2));
    let _bob_stream = subscribe(&bob, Query::from("docs"));
    let mut expected = text(10, LARGE);
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
    alice_up.borrow_mut().clear();
    // Each append is too large for an epoch: it rebuilds the tree, reusing
    // all but its last leaves. The appends go up before any settles.
    for n in 0..8 {
        let suffix = format!("<{n}>{}", "y".repeat(400));
        block_on(alice.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
        expected.push_str(&suffix);
        block_on(alice.tick()).unwrap();
    }
    pump(&server, &[&alice, &bob]);
    let pushed: usize = alice_up
        .borrow()
        .iter()
        .filter(|(kind, ..)| *kind == "push")
        .map(|(_, size, _)| size)
        .sum();
    assert!(pushed > 0);
    assert!(pushed < 8 * LARGE / 2, "pushed {pushed} bytes");
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
    up.borrow_mut().push_back(SyncMessage::Read {
        id: READ,
        query: Box::new(Query::from("titled")),
        binding: BTreeMap::new(),
        include_deleted: false,
        tier: ReadTier::Remote,
        wait: None,
        view: Box::new(ReadViewSourceSpec::Snapshot { snapshot }),
        span: ReadSpan::Once { at: None },
    });
    let reply = || {
        down.borrow().iter().find_map(|message| match message {
            SyncMessage::Settled { id: READ, .. } => Some(Ok(())),
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

// ---- through a relay ------------------------------------------------------

/// Tick a tab, its relay and the relay's upstream until quiet.
fn pump_chain(core: &Db, relay: &Db, tab: &Db) {
    for _ in 0..12 {
        block_on(tab.tick()).unwrap();
        block_on(relay.tick()).unwrap();
        block_on(core.tick()).unwrap();
        block_on(relay.tick()).unwrap();
        block_on(tab.tick()).unwrap();
    }
}

fn requests(log: &Log) -> usize {
    log.borrow()
        .iter()
        .filter(|kind| **kind == "requests")
        .count()
}

#[test]
fn a_range_read_through_a_relay_fetches_only_its_path_at_every_hop() {
    let core = open(0, AuthorSubject::SYSTEM);
    let relay = open(5, user(5));
    let tab = open(6, user(5));
    let upstream = connect(&core, &relay, user(5));
    let downstream = connect(&relay, &tab, user(5));
    let blob: Vec<u8> = (0..LARGE * 16).map(|n| (n * 7 % 251) as u8).collect();
    insert(
        &core,
        "docs",
        1,
        vec![
            ("body", Value::String("small".into())),
            ("blob", Value::Bytes(blob.clone())),
        ],
    );
    let _stream = subscribe(&tab, Query::from("docs").select(["body"]));
    pump_chain(&core, &relay, &tab);
    assert_eq!(relay.large_value_nodes_for_test().unwrap().0, 0);
    assert_eq!(tab.large_value_nodes_for_test().unwrap().0, 0);
    upstream.borrow_mut().clear();
    downstream.borrow_mut().clear();
    let range = 3_000_000..3_000_100;
    let first = block_on(tab.read_value_range("docs", row(1), "blob", range.clone()));
    assert!(first.is_err(), "the first read asks for the range");
    pump_chain(&core, &relay, &tab);
    let got = block_on(tab.read_value_range("docs", row(1), "blob", range.clone()));
    assert_eq!(got.unwrap(), blob[3_000_000..3_000_100]);
    // One pull per hop: the relay forwards what it lacks with the range,
    // and answers the tab from its store once the path arrives.
    assert_eq!(requests(&downstream), 1, "{:?}", downstream.borrow());
    assert_eq!(requests(&upstream), 1, "{:?}", upstream.borrow());
    let (total, _) = core.large_value_nodes_for_test().unwrap();
    let (at_relay, _) = relay.large_value_nodes_for_test().unwrap();
    let (at_tab, _) = tab.large_value_nodes_for_test().unwrap();
    assert!(
        at_relay < total,
        "the relay holds the path: {at_relay} of {total}"
    );
    assert_eq!(at_tab, at_relay, "the tab holds the same path");
    // A second tab reading the same range is answered by the relay alone.
    let other = open(7, user(5));
    let beside = connect(&relay, &other, user(5));
    let _other_stream = subscribe(&other, Query::from("docs").select(["body"]));
    pump_chain(&core, &relay, &other);
    upstream.borrow_mut().clear();
    let _ = block_on(other.read_value_range("docs", row(1), "blob", range.clone()));
    pump_chain(&core, &relay, &other);
    let got = block_on(other.read_value_range("docs", row(1), "blob", range.clone()));
    assert_eq!(got.unwrap(), blob[3_000_000..3_000_100]);
    assert_eq!(requests(&beside), 1);
    assert_eq!(requests(&upstream), 0, "{:?}", upstream.borrow());
}

#[test]
fn a_whole_value_reaches_a_tab_through_its_relay() {
    let core = open(0, AuthorSubject::SYSTEM);
    let relay = open(5, user(5));
    let tab = open(6, user(5));
    connect(&core, &relay, user(5));
    connect(&relay, &tab, user(5));
    let body = text(3, LARGE);
    insert(
        &core,
        "docs",
        1,
        vec![
            ("body", Value::String(body.clone())),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    let _stream = subscribe(&tab, Query::from("docs"));
    pump_chain(&core, &relay, &tab);
    assert_eq!(
        local_cell(&tab, "docs", 1, "body").unwrap(),
        Some(Value::String(body))
    );
}

#[test]
fn a_relay_forwards_no_pull_its_tab_may_not_make() {
    let core = open(0, AuthorSubject::SYSTEM);
    let relay = open(5, user(5));
    let tab = open(9, user(5));
    let upstream = connect(&core, &relay, user(5));
    let (_, up, down) = connect_wires(&relay, &tab, user(5));
    let _relay_stream = subscribe(&relay, Query::from("docs").select(["blob"]));
    let _stream = subscribe(&tab, Query::from("docs").select(["blob"]));
    pump_chain(&core, &relay, &tab);
    insert(
        &core,
        "drafts",
        2,
        vec![("body", Value::String(text(8, LARGE)))],
    );
    insert(
        &core,
        "docs",
        1,
        vec![
            ("body", Value::String(text(1, LARGE))),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    for _ in 0..4 {
        block_on(core.tick()).unwrap();
        block_on(relay.tick()).unwrap();
    }
    let draft = jazz::large_values::prepare(
        jazz::large_values::LargeValueKind::String,
        text(8, LARGE).as_bytes(),
    )
    .unwrap()
    .value_ref
    .root
    .object_hash
    .0;
    let (seq, key) = down
        .borrow()
        .iter()
        .find_map(|message| match message {
            SyncMessage::Rows { images, .. } => images
                .iter()
                .find(|image| image.id == *row(1).0.as_bytes())
                .map(|image| (image.seq, image.key.clone())),
            _ => None,
        })
        .expect("the doc's image");
    down.borrow_mut().clear();
    upstream.borrow_mut().clear();
    let pull = |table: &str, id: u8, seq: u64, root: [u8; 32]| WirePull {
        table: table.to_owned(),
        id: *row(id).0.as_bytes(),
        key: key.clone(),
        seq,
        root,
        nodes: vec![draft],
        span: Some((0, LARGE as u64)),
    };
    let mut pulls = vec![pull("docs", 1, seq, draft)];
    pulls.extend((0..seq + 4).map(|seq| pull("drafts", 2, seq, draft)));
    up.borrow_mut()
        .push_back(SyncMessage::ChunkRequests { pulls });
    for _ in 0..4 {
        block_on(relay.tick()).unwrap();
        block_on(core.tick()).unwrap();
    }
    let mut missing = BTreeSet::new();
    for message in down.borrow().iter() {
        if let SyncMessage::Chunks {
            nodes,
            missing: absent,
        } = message
        {
            assert!(nodes.is_empty(), "nothing is sent");
            missing.extend(absent.iter().map(|node| node.hash));
        }
    }
    assert!(missing.contains(&draft));
    assert_eq!(requests(&upstream), 0, "nothing goes upstream");
    assert_eq!(relay.large_value_nodes_for_test().unwrap().0, 0);
}

#[test]
fn edits_cross_a_relay_as_deltas_both_ways() {
    // A relay serves one identity's sessions (a shared worker and its
    // tabs), so the tab writes as the relay's user.
    let core = open(0, AuthorSubject::SYSTEM);
    let relay = open(5, user(5));
    let tab = open(6, user(5));
    let (relay_up, relay_down) = connect_spied(&core, &relay, user(5));
    let (tab_up, tab_down) = connect_spied(&relay, &tab, user(5));
    let _relay_stream = subscribe(&relay, Query::from("docs"));
    let _stream = subscribe(&tab, Query::from("docs"));
    let mut expected = text(4, LARGE);
    insert(
        &tab,
        "docs",
        1,
        vec![
            ("body", Value::String(expected.clone())),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    pump_chain(&core, &relay, &tab);
    assert_eq!(
        local_cell(&core, "docs", 1, "body").unwrap(),
        Some(Value::String(expected.clone()))
    );
    // The tab's appends go up as deltas over each hop, and the core's
    // come down as deltas over each hop.
    for n in 0..60 {
        let suffix = format!("<{n}>");
        let writer = if n % 2 == 0 { &tab } else { &core };
        block_on(writer.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
        expected.push_str(&suffix);
        pump_chain(&core, &relay, &tab);
    }
    for (wire, kind) in [
        (&tab_up, "commit"),
        (&relay_up, "commit"),
        (&relay_down, "rows"),
        (&tab_down, "rows"),
    ] {
        let wire = wire.borrow();
        let later: Vec<_> = wire.iter().filter(|(k, ..)| *k == kind).skip(2).collect();
        assert!(later.len() >= 20, "{kind}: {wire:?}");
        let small = later
            .iter()
            .filter(|(_, size, delta)| *delta && *size < 512)
            .count();
        assert!(
            small * 10 >= later.len() * 9,
            "{kind}: small deltas: {wire:?}"
        );
    }
    for db in [&core, &relay, &tab] {
        assert_eq!(
            local_cell(db, "docs", 1, "body").unwrap(),
            Some(Value::String(expected.clone()))
        );
    }
}

// ---- partial selections ------------------------------------------------------

fn partial_rows(db: &Db, query: &Query, column: &str) -> Vec<Option<Value>> {
    let rows = block_on(db.all(
        &db.prepare_query(query).unwrap(),
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    rows.iter()
        .map(|row| {
            row.cells()
                .find(|(name, _)| *name == column)
                .and_then(|(_, value)| value.cloned())
        })
        .collect()
}

fn no_rejections(stream: &mut SubscriptionStream) -> usize {
    let mut events = 0;
    while let Some(event) = stream.try_next_event() {
        let text = format!("{event:?}");
        assert!(!text.contains("Rejected"), "{text}");
        events += 1;
    }
    events
}

#[test]
fn a_partial_select_through_a_relay_fetches_only_its_range_and_follows_edits() {
    use jazz::query::Part;
    let core = open(0, AuthorSubject::SYSTEM);
    let relay = open(5, user(5));
    let tab = open(6, user(5));
    let upstream = connect(&core, &relay, user(5));
    let downstream = connect(&relay, &tab, user(5));
    let mut blob: Vec<u8> = (0..LARGE * 16).map(|n| (n * 7 % 251) as u8).collect();
    insert(
        &core,
        "docs",
        1,
        vec![
            ("body", Value::String("small".into())),
            ("blob", Value::Bytes(blob.clone())),
        ],
    );
    let range = 3_000_000..3_000_100;
    let query = Query::from("docs").select(["blob"]).select_part(
        "blob",
        Part::Bytes {
            from: range.start as u64,
            to: range.end as u64,
        },
    );
    let mut stream = subscribe(&tab, query.clone());
    pump_chain(&core, &relay, &tab);
    assert!(no_rejections(&mut stream) > 0, "the subscription opened");
    assert_eq!(
        partial_rows(&tab, &query, "blob"),
        vec![Some(Value::Bytes(blob[range.clone()].to_vec()))]
    );
    let (total, _) = core.large_value_nodes_for_test().unwrap();
    let (at_relay, _) = relay.large_value_nodes_for_test().unwrap();
    let (at_tab, _) = tab.large_value_nodes_for_test().unwrap();
    assert!(
        at_relay < total,
        "the relay holds the path: {at_relay} of {total}"
    );
    assert!(at_tab <= at_relay, "the tab holds the path: {at_tab}");
    // The range's whole path in one pull per hop.
    assert_eq!(requests(&downstream), 1, "{:?}", downstream.borrow());
    assert_eq!(requests(&upstream), 1, "{:?}", upstream.borrow());
    upstream.borrow_mut().clear();
    downstream.borrow_mut().clear();
    // Edits outside the range and inside it reach the tab as deltas the
    // range reads through: nothing more is fetched.
    for n in 0..10u8 {
        block_on(core.append_value("docs", row(1), "blob", vec![n; 16])).unwrap();
        blob.extend([n; 16]);
        pump_chain(&core, &relay, &tab);
    }
    block_on(core.splice_value(
        "docs",
        row(1),
        "blob",
        jazz::db::LargeValueUpdateSplice {
            at: 3_000_010,
            delete: 5,
            insert: b"edited".to_vec(),
        },
        Default::default(),
    ))
    .unwrap();
    blob.splice(3_000_010..3_000_015, b"edited".iter().copied());
    pump_chain(&core, &relay, &tab);
    assert!(no_rejections(&mut stream) > 0, "the edits were delivered");
    assert_eq!(
        partial_rows(&tab, &query, "blob"),
        vec![Some(Value::Bytes(blob[range.clone()].to_vec()))]
    );
    assert_eq!(requests(&downstream), 0, "{:?}", downstream.borrow());
    assert_eq!(requests(&upstream), 0, "{:?}", upstream.borrow());
    assert_eq!(tab.large_value_nodes_for_test().unwrap().0, at_tab);
}

#[test]
fn a_partial_select_in_an_include_fetches_only_its_range_through_a_relay() {
    use jazz::query::{ArraySubquery, Part};
    use jazz::result_tree::ResultRelation;
    let core = open(0, AuthorSubject::SYSTEM);
    let relay = open(5, user(5));
    let tab = open(6, user(5));
    let upstream = connect(&core, &relay, user(5));
    let downstream = connect(&relay, &tab, user(5));
    let mut blob: Vec<u8> = (0..LARGE * 16).map(|n| (n * 11 % 251) as u8).collect();
    insert(
        &core,
        "docs",
        1,
        vec![
            ("body", Value::String("small".into())),
            ("blob", Value::Bytes(blob.clone())),
        ],
    );
    insert(&core, "links", 2, vec![("doc", Value::Uuid(row(1).0))]);
    let range = 2_000_000..2_000_080;
    let query = Query::from("links").array_subquery(
        ArraySubquery::new("doc", "docs", "id", "doc")
            .select(["blob"])
            .select_part(
                "blob",
                Part::Bytes {
                    from: range.start as u64,
                    to: range.end as u64,
                },
            ),
    );
    let included = |db: &Db| {
        let tree = block_on(db.all_result_tree(
            &db.prepare_query(&query).unwrap(),
            ReadOpts {
                tier: ReadTier::LocalOnly,
                ..ReadOpts::default()
            },
        ))
        .unwrap();
        tree.roots
            .iter()
            .flat_map(|node| {
                let ResultRelation::Array(children) = &node.relations["doc"] else {
                    panic!("expected an array relation");
                };
                children
                    .iter()
                    .map(|child| {
                        child
                            .row
                            .cells()
                            .find(|(name, _)| *name == "blob")
                            .and_then(|(_, value)| value.cloned())
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    let mut stream = subscribe(&tab, query.clone());
    pump_chain(&core, &relay, &tab);
    assert!(no_rejections(&mut stream) > 0, "the subscription opened");
    assert_eq!(
        included(&tab),
        vec![Some(Value::Bytes(blob[range.clone()].to_vec()))]
    );
    let (total, _) = core.large_value_nodes_for_test().unwrap();
    let (at_relay, _) = relay.large_value_nodes_for_test().unwrap();
    let (at_tab, _) = tab.large_value_nodes_for_test().unwrap();
    assert!(
        at_relay < total,
        "the relay holds the path: {at_relay} of {total}"
    );
    assert!(at_tab <= at_relay, "the tab holds the path: {at_tab}");
    assert_eq!(requests(&downstream), 1, "{:?}", downstream.borrow());
    assert_eq!(requests(&upstream), 1, "{:?}", upstream.borrow());
    upstream.borrow_mut().clear();
    downstream.borrow_mut().clear();
    // An edit inside the included range arrives as a delta it reads through.
    block_on(core.splice_value(
        "docs",
        row(1),
        "blob",
        jazz::db::LargeValueUpdateSplice {
            at: 2_000_020,
            delete: 3,
            insert: b"included".to_vec(),
        },
        Default::default(),
    ))
    .unwrap();
    blob.splice(2_000_020..2_000_023, b"included".iter().copied());
    pump_chain(&core, &relay, &tab);
    assert!(no_rejections(&mut stream) > 0, "the edit was delivered");
    assert_eq!(
        included(&tab),
        vec![Some(Value::Bytes(blob[range.clone()].to_vec()))]
    );
    assert_eq!(requests(&downstream), 0, "{:?}", downstream.borrow());
    assert_eq!(requests(&upstream), 0, "{:?}", upstream.borrow());
    assert_eq!(tab.large_value_nodes_for_test().unwrap().0, at_tab);
}

#[test]
fn partial_selects_read_only_their_part_of_text_and_json() {
    use jazz::query::Part;
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    connect(&server, &alice, user(1));
    let body: String = text(5, LARGE * 4)
        .chars()
        .enumerate()
        .map(|(n, c)| if n % 3 == 1 { 'é' } else { c })
        .collect();
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String(body.clone())),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    let (total, _) = server.large_value_nodes_for_test().unwrap();
    let utf16 = Query::from("docs").select(["body"]).select_part(
        "body",
        Part::TextUtf16 {
            from: 700_000,
            to: 700_050,
        },
    );
    let mut stream = subscribe(&alice, utf16.clone());
    pump(&server, &[&alice]);
    no_rejections(&mut stream);
    let expected: String = body.chars().skip(700_000).take(50).collect();
    assert_eq!(
        partial_rows(&alice, &utf16, "body"),
        vec![Some(Value::String(expected))]
    );
    let (after_utf16, _) = alice.large_value_nodes_for_test().unwrap();
    assert!(after_utf16 < total, "{after_utf16} of {total}");
    // UTF-8 bytes on code point boundaries; one splitting one is refused.
    let start = body.char_indices().nth(300_000).unwrap().0 as u64;
    let end = body.char_indices().nth(300_020).unwrap().0 as u64;
    let utf8 = Query::from("docs").select(["body"]).select_part(
        "body",
        Part::TextUtf8 {
            from: start,
            to: end,
        },
    );
    // A second subscription to the same rows in another part reads its
    // own part; the first keeps its own.
    let mut utf8_stream = subscribe(&alice, utf8.clone());
    pump(&server, &[&alice]);
    assert!(no_rejections(&mut utf8_stream) > 0);
    let got = partial_rows(&alice, &utf8, "body");
    let Some(Some(Value::String(got))) = got.first().cloned() else {
        panic!("{got:?}");
    };
    assert_eq!(got, body[start as usize..end as usize]);
    let (after_utf8, _) = alice.large_value_nodes_for_test().unwrap();
    assert!(after_utf8 < total, "{after_utf8} of {total}");
    let expected: String = body.chars().skip(700_000).take(50).collect();
    assert_eq!(
        partial_rows(&alice, &utf16, "body"),
        vec![Some(Value::String(expected))]
    );
    let split = Query::from("docs").select(["body"]).select_part(
        "body",
        Part::TextUtf8 {
            from: start + 1,
            to: end,
        },
    );
    let error = block_on(alice.all(
        &alice.prepare_query(&split).unwrap(),
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ));
    assert!(
        format!("{error:?}").contains("splits a code point")
            || body.is_char_boundary(start as usize + 1),
        "{error:?}"
    );
}

#[test]
fn a_json_pointer_select_reads_its_subtree() {
    use jazz::query::Part;
    let schema = || {
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("docs")
                        .column("doc", ColumnType::Json { schema: None })
                        .policies(allow_all_policies()),
                )
                .build(),
        )
    };
    let open_json = |node: u8, author: AuthorSubject| {
        block_on(Db::open(DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let server = open_json(0, AuthorSubject::SYSTEM);
    let alice = open_json(1, user(1));
    connect(&server, &alice, user(1));
    // A column selected by JSON pointer cannot take splices yet.
    let pointer = Query::from("docs")
        .select(["doc"])
        .select_part("doc", Part::JsonPointer("/0".into()));
    let refused = block_on(alice.subscribe(
        &alice.prepare_query(&pointer).unwrap(),
        ReadOpts::default().with_splices(["doc"]),
    ));
    assert!(refused.is_err());
    let items: Vec<serde_json::Value> = (0..20_000)
        .map(|n| serde_json::json!({ "n": n, "text": text(n, 40) }))
        .collect();
    let doc = serde_json::json!(items);
    insert(
        &server,
        "docs",
        1,
        vec![("doc", Value::String(doc.to_string()))],
    );
    let query = Query::from("docs")
        .select(["doc"])
        .select_part("doc", Part::JsonPointer("/3/text".into()));
    let _stream = subscribe(&alice, query.clone());
    pump(&server, &[&alice]);
    assert_eq!(
        partial_rows(&alice, &query, "doc"),
        vec![Some(Value::String(
            serde_json::json!(text(3, 40)).to_string()
        ))]
    );
    let (after, _) = alice.large_value_nodes_for_test().unwrap();
    let (total, _) = server.large_value_nodes_for_test().unwrap();
    assert!(
        after < total,
        "a leading array index reads a prefix: {after} of {total}"
    );
}

/// `edits` applied in order to `bytes`.
fn apply_delta(bytes: &[u8], edits: &[jazz::db::LargeValueDeltaSplice]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for edit in edits {
        let at = edit.at as usize;
        out.splice(at..at + edit.delete as usize, edit.insert.iter().copied());
    }
    out
}

/// `edits` applied in order to `text` by their UTF-16 coordinates.
fn apply_delta_utf16(text: &str, edits: &[jazz::db::LargeValueDeltaSplice]) -> String {
    let mut units: Vec<u16> = text.encode_utf16().collect();
    for edit in edits {
        let at = edit.at_utf16 as usize;
        let insert = std::str::from_utf8(&edit.insert).unwrap();
        units.splice(at..at + edit.delete_utf16 as usize, insert.encode_utf16());
    }
    String::from_utf16(&units).unwrap()
}

#[test]
fn value_delta_turns_one_version_into_another() {
    let db = open(0, AuthorSubject::SYSTEM);
    let v1_text: String = text(11, LARGE * 8)
        .chars()
        .enumerate()
        .map(|(n, c)| if n % 97 == 0 { '😀' } else { c })
        .collect();
    let v1 = block_on(
        db.insert(
            "docs",
            [
                ("body".to_owned(), Value::String(v1_text.clone())),
                ("blob".to_owned(), Value::Bytes(Vec::new())),
            ]
            .into(),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ),
    )
    .unwrap()
    .mergeable_tx_id();
    let at = v1_text.char_indices().nth(1_000_000).unwrap().0 as u64;
    let v2 = block_on(db.splice_value(
        "docs",
        row(1),
        "body",
        jazz::db::LargeValueUpdateSplice {
            at,
            delete: 4,
            insert: "spliced é".as_bytes().to_vec(),
        },
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id();
    let v3 = block_on(db.append_value("docs", row(1), "body", " and appended".into()))
        .unwrap()
        .mergeable_tx_id();
    let mut v3_text = v1_text.clone();
    v3_text.replace_range(at as usize..at as usize + 4, "spliced é");
    v3_text.push_str(" and appended");
    let forward = block_on(db.value_delta("docs", row(1), "body", v1, v3)).unwrap();
    assert_eq!(forward.len(), 2, "{forward:?}");
    assert_eq!(
        apply_delta(v1_text.as_bytes(), &forward),
        v3_text.as_bytes()
    );
    assert_eq!(apply_delta_utf16(&v1_text, &forward), v3_text);
    let back = block_on(db.value_delta("docs", row(1), "body", v3, v2)).unwrap();
    let mut v2_text = v3_text.clone();
    v2_text.truncate(v2_text.len() - " and appended".len());
    assert_eq!(apply_delta_utf16(&v3_text, &back), v2_text);
    // A whole new value with a small change is a small delta.
    let mut v4_text = v3_text.clone();
    let mid = v4_text.char_indices().nth(500_000).unwrap().0;
    v4_text.replace_range(mid..mid, "a whole new value ");
    let v4 = block_on(db.update(
        "docs",
        row(1),
        [("body".to_owned(), Value::String(v4_text.clone()))].into(),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id();
    let rewritten = block_on(db.value_delta("docs", row(1), "body", v3, v4)).unwrap();
    assert_eq!(apply_delta_utf16(&v3_text, &rewritten), v4_text);
    let moved: usize = rewritten
        .iter()
        .map(|edit| edit.delete as usize + edit.insert.len())
        .sum();
    assert!(moved < v4_text.len() / 10, "{moved} of {}", v4_text.len());
    // An inline side is one whole replace.
    let v5 = block_on(db.update(
        "docs",
        row(1),
        [("body".to_owned(), Value::String("short".into()))].into(),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id();
    let to_inline = block_on(db.value_delta("docs", row(1), "body", v4, v5)).unwrap();
    assert_eq!(apply_delta_utf16(&v4_text, &to_inline), "short");
    let from_inline = block_on(db.value_delta("docs", row(1), "body", v5, v1)).unwrap();
    assert_eq!(apply_delta_utf16("short", &from_inline), v1_text);
    // Between inline values: nothing when they are equal, else only the
    // characters that differ.
    let inline = |value: &str| {
        block_on(db.update(
            "docs",
            row(1),
            [("body".to_owned(), Value::String(value.into()))].into(),
            Default::default(),
        ))
        .unwrap()
        .mergeable_tx_id()
    };
    let (v6, v7) = (inline("short é text"), inline("short é text"));
    assert_eq!(
        block_on(db.value_delta("docs", row(1), "body", v6, v7)).unwrap(),
        []
    );
    let v8 = inline("short ü text");
    let narrow = block_on(db.value_delta("docs", row(1), "body", v7, v8)).unwrap();
    assert_eq!(apply_delta_utf16("short é text", &narrow), "short ü text");
    assert_eq!(
        (narrow.len(), narrow[0].at_utf16, narrow[0].delete_utf16),
        (1, 6, 1)
    );
}

#[test]
fn value_delta_needs_both_versions_readable() {
    let db = open(1, user(1));
    let handle = block_on(db.insert(
        "drafts",
        [("body".to_owned(), Value::String(text(12, LARGE * 2)))].into(),
        InsertOptions {
            row_id: Some(row(3)),
            ..Default::default()
        },
    ))
    .unwrap();
    let seq = handle.mergeable_tx_id();
    let error = block_on(db.value_delta("drafts", row(3), "body", seq, seq)).unwrap_err();
    assert!(error.to_string().contains("not readable"), "{error}");
    let system = open(0, AuthorSubject::SYSTEM);
    let missing = block_on(system.value_delta("docs", row(4), "body", 1, 2)).unwrap_err();
    assert!(missing.to_string().contains("not held"), "{missing}");
}

/// The `column` splices of every updated row in the events `stream` holds.
fn splices_of(
    stream: &mut SubscriptionStream,
    column: &str,
) -> Vec<Option<Vec<jazz::db::LargeValueDeltaSplice>>> {
    let mut out = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta { updated, .. } = event {
            for row in updated {
                out.push(
                    row.splices
                        .iter()
                        .find(|(name, _)| name == column)
                        .map(|(_, splices)| splices.clone()),
                );
            }
        }
    }
    out
}

#[test]
fn subscriptions_take_a_columns_changes_as_splices() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    connect(&server, &alice, user(1));
    let mut body = text(13, LARGE * 6);
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String(body.clone())),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    let query = Query::from("docs").select(["body"]);
    let mut stream = block_on(alice.subscribe(
        &alice.prepare_query(&query).unwrap(),
        ReadOpts::default().with_splices(["body"]),
    ))
    .unwrap();
    pump(&server, &[&alice]);
    no_rejections(&mut stream);
    // A remote edit arrives with its splices from the previous result.
    block_on(server.splice_value(
        "docs",
        row(1),
        "body",
        jazz::db::LargeValueUpdateSplice {
            at: 1_000_000,
            delete: 3,
            insert: "remote é".as_bytes().to_vec(),
        },
        Default::default(),
    ))
    .unwrap();
    pump(&server, &[&alice]);
    let next = {
        let mut next = body.clone();
        next.replace_range(1_000_000..1_000_003, "remote é");
        next
    };
    let remote = splices_of(&mut stream, "body");
    assert_eq!(remote.len(), 1, "{remote:?}");
    let splices = remote[0].clone().expect("the edit came as splices");
    assert_eq!(splices.len(), 1);
    assert_eq!(apply_delta_utf16(&body, &splices), next);
    body = next;
    // So does a local one, and the settlement that follows it.
    block_on(alice.append_value("docs", row(1), "body", " local".into())).unwrap();
    block_on(alice.tick()).unwrap();
    let local = splices_of(&mut stream, "body");
    assert_eq!(local.len(), 1, "{local:?}");
    let splices = local[0].clone().expect("the local edit came as splices");
    let appended = format!("{body} local");
    assert_eq!(apply_delta_utf16(&body, &splices), appended);
    pump(&server, &[&alice]);
    for splices in splices_of(&mut stream, "body").into_iter().flatten() {
        assert_eq!(apply_delta_utf16(&appended, &splices), appended);
    }
    // Only large-capable columns can be asked for.
    let refused = block_on(alice.subscribe(
        &alice.prepare_query(&query).unwrap(),
        ReadOpts::default().with_splices(["id"]),
    ));
    assert!(refused.is_err());
}

#[test]
fn a_range_select_takes_its_pages_changes_as_page_splices() {
    use jazz::query::Part;
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    connect(&server, &alice, user(1));
    let mut body = text(14, LARGE * 6);
    insert(
        &server,
        "docs",
        1,
        vec![
            ("body", Value::String(body.clone())),
            ("blob", Value::Bytes(Vec::new())),
        ],
    );
    let page = 500_000..500_040;
    let query = Query::from("docs").select(["body"]).select_part(
        "body",
        Part::TextUtf16 {
            from: page.start as u64,
            to: page.end as u64,
        },
    );
    let mut stream = block_on(alice.subscribe(
        &alice.prepare_query(&query).unwrap(),
        ReadOpts::default().with_splices(["body"]),
    ))
    .unwrap();
    pump(&server, &[&alice]);
    no_rejections(&mut stream);
    let shown = |body: &str| body[page.clone()].to_owned();
    let before = shown(&body);
    // An edit inside the page is one page-relative splice.
    block_on(server.splice_value(
        "docs",
        row(1),
        "body",
        jazz::db::LargeValueUpdateSplice {
            at: 500_010,
            delete: 2,
            insert: "XY".as_bytes().to_vec(),
        },
        Default::default(),
    ))
    .unwrap();
    body.replace_range(500_010..500_012, "XY");
    pump(&server, &[&alice]);
    let got = splices_of(&mut stream, "body");
    assert_eq!(got.len(), 1, "{got:?}");
    let splices = got[0].clone().expect("page splices");
    assert_eq!(splices.len(), 1);
    assert_eq!((splices[0].at_utf16, splices[0].delete_utf16), (10, 2));
    assert_eq!(apply_delta_utf16(&before, &splices), shown(&body));
    // An insertion before the page shifts the window's content into it.
    let before = shown(&body);
    block_on(server.splice_value(
        "docs",
        row(1),
        "body",
        jazz::db::LargeValueUpdateSplice {
            at: 10,
            delete: 0,
            insert: "+++".as_bytes().to_vec(),
        },
        Default::default(),
    ))
    .unwrap();
    body.replace_range(10..10, "+++");
    pump(&server, &[&alice]);
    for splices in splices_of(&mut stream, "body") {
        let splices = splices.expect("page splices");
        assert_eq!(apply_delta_utf16(&before, &splices), shown(&body));
    }
}

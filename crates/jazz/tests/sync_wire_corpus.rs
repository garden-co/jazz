//! The pinned SQLite sync wire corpus: `fixtures/sqlite_sync_wire_v1.json`.
//!
//! After the `Hello`, a connection carries postcard [`SyncMessage`] frames
//! (and, server to client, `WireFrame::Error`) in websocket frame batches.
//! The corpus holds one real frame of every `SyncMessage` variant, recorded
//! from a scenario that sends each of them through the production encoder,
//! plus a rejected `Fate` with and without its reason name, `Hello` and
//! `Error` frames, and frames a reader must reject. The
//! jazz-tools node test `wire-frame-artifact-corpus.test.ts` runs every one
//! through the NAPI and WASM artifacts
//! ([`jazz::db::validate_frame_batch_for_corpus`]); this file checks that
//! each pinned frame still decodes to its variant and re-encodes to the
//! same bytes, so a change to the message layout fails here first.
//!
//! The wire has not shipped yet: until alpha.60 a layout change re-runs the
//! producer (`JAZZ_WRITE_SQLITE_SYNC_WIRE_CORPUS=1`). After it, a change
//! needs a new wire version and corpus beside this one.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    CorpusReader, Db, DbConfig, DbIdentity, InsertOptions, Outcome, ReadOpts, ReadSpan, ReadTier,
    Reason, SyncMessage, Transport, UpdateOptions, block_on, encode_frames,
    validate_frame_batch_for_corpus,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::model::transaction::OpenTransactionId;
use jazz::protocol::{LensOp, MigrationLens, PermissionAdviceAction, TableLens};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::wire::{
    TransportError, WireError, WireErrorCode, WireFrame, WireHello, WirePeerRole, WireRetry,
    current_wire_features, encode_frame, encode_websocket_frame_batch,
};
use serde::{Deserialize, Serialize};

use common::{compile_schema, read_and_allow_all_writes, session_eq};

const CORPUS: &str = "sqlite_sync_wire_v1.json";
const FORMAT: &str = "jazz-sqlite-sync-wire-corpus-v1";

/// Every `SyncMessage` variant, in declaration order. A message too
/// large for one frame travels in parts below the message layer
/// (binding_host's frame-split test covers it).
const VARIANTS: [&str; 19] = [
    "Read",
    "Unsubscribe",
    "Hold",
    "Settled",
    "Commit",
    "ChunkRequests",
    "PushChunks",
    "Chunks",
    "Rows",
    "Evict",
    "Refused",
    "Fate",
    "Advise",
    "Advice",
    "Schema",
    "SchemaAdmission",
    "Stored",
    "Holds",
    "Catalogue",
];

#[derive(Debug, Serialize, Deserialize)]
struct Corpus {
    format: String,
    /// One recorded frame per variant.
    messages: Vec<Message>,
    /// Recorded `Fate` frames of rejected commits, one per shape of
    /// rejection: with a reason name and without one.
    fates: Vec<Fate>,
    /// Complete batches a reader accepts that are not sync messages.
    frames: Vec<Batch>,
    /// Batches a reader must reject.
    rejections: Vec<Batch>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    variant: String,
    reader: String,
    frame_hex: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Fate {
    name: String,
    /// The rejection's reason name, or `None` for a rejection without one.
    reason: Option<String>,
    reader: String,
    frame_hex: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Batch {
    name: String,
    reader: String,
    batch_hex: String,
}

fn reader(name: &str) -> CorpusReader {
    match name {
        "server-opening" => CorpusReader::ServerOpening,
        "server" => CorpusReader::Server,
        "client" => CorpusReader::Client,
        other => panic!("unknown corpus reader {other}"),
    }
}

fn variant(message: &SyncMessage) -> String {
    format!("{message:?}")
        .chars()
        .take_while(char::is_ascii_alphanumeric)
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

fn corpus() -> Corpus {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(CORPUS);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn every_pinned_sync_frame_decodes_to_its_variant_and_reencodes_exactly() {
    let corpus = corpus();
    assert_eq!(corpus.format, FORMAT);
    let variants: Vec<&str> = corpus.messages.iter().map(|m| m.variant.as_str()).collect();
    assert_eq!(variants, VARIANTS, "one frame of every variant, in order");
    for message in &corpus.messages {
        let bytes = unhex(&message.frame_hex);
        let decoded: SyncMessage = postcard::from_bytes(&bytes).unwrap_or_else(|error| {
            panic!(
                "{}: the pinned frame no longer decodes ({error}); the sync wire layout changed",
                message.variant
            )
        });
        assert_eq!(variant(&decoded), message.variant);
        assert_eq!(
            postcard::to_allocvec(&decoded).unwrap(),
            bytes,
            "{}: re-encoding changed the bytes; the sync wire layout changed",
            message.variant
        );
    }
}

/// The rejected-fate layout (a reason name beside the message) is pinned by
/// real frames: each still decodes to a rejection with its reason name and
/// re-encodes to the same bytes.
#[test]
fn every_pinned_rejected_fate_decodes_with_its_reason_and_reencodes_exactly() {
    let corpus = corpus();
    let reasons: Vec<Option<&str>> = corpus.fates.iter().map(|f| f.reason.as_deref()).collect();
    assert_eq!(
        reasons,
        [Some(Reason::ClientClockTooFarAhead.as_str()), None],
        "a rejection with a reason name and one without"
    );
    for fate in &corpus.fates {
        let bytes = unhex(&fate.frame_hex);
        let decoded: SyncMessage = postcard::from_bytes(&bytes).unwrap_or_else(|error| {
            panic!(
                "{}: the pinned frame no longer decodes ({error}); the fate layout changed",
                fate.name
            )
        });
        let SyncMessage::Fate {
            outcome: Outcome::Rejected { reason, message },
            ..
        } = &decoded
        else {
            panic!("{}: not a rejected fate: {decoded:?}", fate.name);
        };
        assert_eq!(reason, &fate.reason, "{}", fate.name);
        assert!(!message.is_empty(), "{}", fate.name);
        assert_eq!(
            postcard::to_allocvec(&decoded).unwrap(),
            bytes,
            "{}: re-encoding changed the bytes; the fate layout changed",
            fate.name
        );
    }
}

/// The shapes one frame per variant misses are pinned too: each still
/// decodes to its shape and re-encodes to the same bytes.
#[test]
fn every_pinned_message_shape_decodes_and_reencodes_exactly() {
    let corpus = corpus();
    for name in SHAPES {
        let entry = corpus
            .frames
            .iter()
            .find(|frame| frame.name == name)
            .unwrap_or_else(|| panic!("{name} is not pinned"));
        let frames = jazz::wire::decode_websocket_frame_batch(&unhex(&entry.batch_hex)).unwrap();
        let [bytes] = frames.as_slice() else {
            panic!("{name}: one frame");
        };
        let decoded: SyncMessage = postcard::from_bytes(bytes).unwrap_or_else(|error| {
            panic!("{name}: the pinned frame no longer decodes ({error}); the layout changed")
        });
        assert_eq!(shape(&decoded), Some(name));
        assert_eq!(
            &postcard::to_allocvec(&decoded).unwrap(),
            bytes,
            "{name}: re-encoding changed the bytes; the layout changed"
        );
    }
}

#[test]
fn the_corpus_validator_accepts_every_frame_and_rejects_every_rejection() {
    let corpus = corpus();
    let features = current_wire_features();
    for message in &corpus.messages {
        let batch = encode_websocket_frame_batch(&[unhex(&message.frame_hex)]).unwrap();
        validate_frame_batch_for_corpus(&batch, reader(&message.reader), features)
            .unwrap_or_else(|error| panic!("{}: {error}", message.variant));
    }
    for fate in &corpus.fates {
        let batch = encode_websocket_frame_batch(&[unhex(&fate.frame_hex)]).unwrap();
        validate_frame_batch_for_corpus(&batch, reader(&fate.reader), features)
            .unwrap_or_else(|error| panic!("{}: {error}", fate.name));
    }
    for frame in &corpus.frames {
        validate_frame_batch_for_corpus(&unhex(&frame.batch_hex), reader(&frame.reader), features)
            .unwrap_or_else(|error| panic!("{}: {error}", frame.name));
    }
    for rejection in &corpus.rejections {
        assert!(
            validate_frame_batch_for_corpus(
                &unhex(&rejection.batch_hex),
                reader(&rejection.reader),
                features,
            )
            .is_err(),
            "{} was accepted",
            rejection.name
        );
    }
}

// ---- producing the corpus --------------------------------------------------

type Recorded = Rc<RefCell<BTreeMap<String, (String, Vec<u8>)>>>;

/// One side of a duplex link that sends every message through the
/// production frame encoder and hands the peer the decoded frames,
/// recording the first frame of each variant.
struct Recording {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    /// The reader of what this side sends.
    peer: &'static str,
    recorded: Recorded,
}

impl Transport for Recording {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        for frame in encode_frames(message).unwrap() {
            let decoded: SyncMessage = postcard::from_bytes(&frame).unwrap();
            // The pinned Commit is the first that relays a session's write
            // built on an unsettled commit (bases, origin), else the first.
            let relayed = matches!(
                &decoded,
                SyncMessage::Commit { bases, origin: Some(_), .. }
                    if bases.iter().any(|base| matches!(base.on, jazz::db::BuiltOn::Commit { .. }))
            );
            let mut recorded = self.recorded.borrow_mut();
            if let SyncMessage::Fate {
                outcome: Outcome::Rejected { reason, .. },
                ..
            } = &decoded
            {
                recorded
                    .entry(rejected_fate_key(reason.as_deref()))
                    .or_insert_with(|| (self.peer.to_owned(), frame.clone()));
            }
            if let Some(shape) = shape(&decoded) {
                recorded
                    .entry(shape.to_owned())
                    .or_insert_with(|| (self.peer.to_owned(), frame.clone()));
            }
            let pinned = recorded
                .entry(variant(&decoded))
                .or_insert_with(|| (self.peer.to_owned(), frame.clone()));
            let pinned_relayed = matches!(
                postcard::from_bytes(&pinned.1),
                Ok(SyncMessage::Commit { bases, origin: Some(_), .. })
                    if bases.iter().any(|base: &jazz::db::WireBase| matches!(base.on, jazz::db::BuiltOn::Commit { .. }))
            );
            if relayed && !pinned_relayed {
                *pinned = (self.peer.to_owned(), frame.clone());
            }
            drop(recorded);
            self.outbound.borrow_mut().push_back(decoded);
        }
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// Message shapes the one frame per variant leaves unpinned: a one-shot
/// `Read` (its span carries a snapshot and a view), `Rows` carrying the
/// commits behind history versions, and a `Catalogue` carrying versions
/// with their stored names and a lens's cells.
const SHAPES: [&str; 3] = ["Read/once", "Rows/origins", "Catalogue/versions"];

fn shape(message: &SyncMessage) -> Option<&'static str> {
    match message {
        SyncMessage::Read {
            span: ReadSpan::Once { .. },
            ..
        } => Some(SHAPES[0]),
        SyncMessage::Rows { origins, .. } if !origins.is_empty() => Some(SHAPES[1]),
        SyncMessage::Catalogue {
            versions, cells, ..
        } if !versions.is_empty() && !cells.is_empty() => Some(SHAPES[2]),
        _ => None,
    }
}

/// The two-frame split of `frame` a reader joins: the transport's part
/// markers (0xFF a leading part, 0xFE the last) ahead of each half. A real
/// split happens past 2 MiB; the joiner reads a small one the same way.
fn split(frame: &[u8]) -> [Vec<u8>; 2] {
    let (head, tail) = frame.split_at(frame.len() / 2);
    [[&[0xFF], head].concat(), [&[0xFE], tail].concat()]
}

/// The recording key of the first rejected fate with `reason`.
fn rejected_fate_key(reason: Option<&str>) -> String {
    format!("rejected Fate ({})", reason.unwrap_or("no reason"))
}

fn connect(client: &Db, server: &Db, author: AuthorSubject, recorded: &Recorded) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    block_on(client.connect_upstream(Box::new(Recording {
        outbound: up.clone(),
        inbound: down.clone(),
        peer: "server",
        recorded: recorded.clone(),
    })));
    server.accept_subscriber(
        Box::new(Recording {
            outbound: down,
            inbound: up,
            peer: "client",
            recorded: recorded.clone(),
        }),
        author,
    );
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x40 + n; 16])
}

fn subject(n: u8) -> Value {
    Value::String(user(n).principal_parts().1)
}

const ME: [&str; 3] = ["user", "identity", "subject"];

/// `notes(owner, body)`, readable by their owner. `draft` adds a column,
/// a schema version the server has not admitted.
fn schema(draft: bool) -> JazzSchema {
    let mut notes = TableSchemaBuilder::new("notes")
        .column("owner", ColumnType::Text)
        .column("body", ColumnType::Bytea);
    if draft {
        notes = notes.column("title", ColumnType::Text);
    }
    compile_schema(
        &SchemaBuilder::new()
            .table(notes.policies(read_and_allow_all_writes(session_eq("owner", &ME))))
            .build(),
    )
}

/// `schema(false)` with `first_owner`, a lens's copy of `owner`.
fn schema_with_copy() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("body", ColumnType::Bytea)
                    .column("first_owner", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq("owner", &ME))),
            )
            .build(),
    )
}

fn note(owner: u8, body: Vec<u8>) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), subject(owner)),
        ("body".to_owned(), Value::Bytes(body)),
    ])
}

fn open(node: u8, author: AuthorSubject, draft: bool) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(draft),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn pump(dbs: &[&Db]) {
    for _ in 0..12 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

/// Just over the inline limit, so it travels as chunks; uniform, so the
/// pinned frames stay small.
fn large(seed: u8) -> Vec<u8> {
    vec![seed; jazz::large_values::INLINE_VALUE_MAX_BYTES + 1]
}

/// A scenario that sends every `SyncMessage` variant at least once.
fn record() -> BTreeMap<String, (String, Vec<u8>)> {
    let recorded: Recorded = Rc::default();
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(false),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let shared = RowUuid::from_bytes([0x11; 16]);
    block_on(server.insert(
        "notes",
        note(1, large(1)),
        InsertOptions {
            row_id: Some(shared),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(server.update("notes", shared, note(1, large(2)), UpdateOptions::default())).unwrap();

    // Schema, SchemaAdmission, Read (live), Rows, Settled, ChunkRequests,
    // Chunks: a client subscribes and pulls the large value it reads.
    let alice = open(0x51, user(1), false);
    connect(&alice, &server, user(1), &recorded);
    let notes = alice.prepare_query(&Query::from("notes")).unwrap();
    let stream = block_on(alice.subscribe(&notes, ReadOpts::default())).unwrap();
    pump(&[&alice, &server, &alice]);
    let _ = block_on(alice.all(&notes, ReadOpts::default())).unwrap();
    pump(&[&alice, &server, &alice]);

    // PushChunks, Commit, Fate: a write with a new large value.
    let mine = RowUuid::from_bytes([0x12; 16]);
    block_on(alice.insert(
        "notes",
        note(1, large(3)),
        InsertOptions {
            row_id: Some(mine),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&[&alice, &server, &alice]);

    // Read (once), Settled with a listing: a serializable
    // transaction's second read is pinned to its snapshot, so after a
    // newer write it answers from history.
    let open_tx = OpenTransactionId::new();
    block_on(alice.begin_serializable(open_tx)).unwrap();
    let _ = drive(
        &[&alice, &server, &alice],
        alice.tx_ref(open_tx).all_prepared(&notes),
    );
    block_on(server.update("notes", shared, note(1, large(4)), UpdateOptions::default())).unwrap();
    pump(&[&alice, &server, &alice]);
    let _ = drive(
        &[&alice, &server, &alice],
        alice.tx_ref(open_tx).all_prepared(&notes),
    );
    alice.abandon_transaction_handle(open_tx).unwrap();

    // A history subscription ships superseded versions, with the commits
    // behind them.
    let history = alice
        .prepare_query(
            &Query::from("history.notes").filter(eq(col("id"), lit(Value::Uuid(shared.0)))),
        )
        .unwrap();
    let _history = block_on(alice.subscribe(&history, ReadOpts::default())).unwrap();
    pump(&[&alice, &server, &alice]);

    // Advise, Advice.
    let id = alice.request_permission_advice(
        PermissionAdviceAction::Read {
            table: "notes".to_owned(),
            row: shared,
        },
        None,
    );
    let _ = drive(&[&alice, &server, &alice], alice.permission_advice(id));

    // The server gives the shared note away while alice holds it: evicted.
    block_on(server.update(
        "notes",
        shared,
        BTreeMap::from([("owner".to_owned(), subject(2))]),
        UpdateOptions::default(),
    ))
    .unwrap();
    pump(&[&alice, &server, &alice]);

    // Unsubscribe.
    drop(stream);
    pump(&[&alice, &server, &alice]);

    // Hold, Evict: alice's own note is deleted while she is away; she
    // reconnects holding it.
    block_on(server.delete("notes", mine, Default::default())).unwrap();
    connect(&alice, &server, user(1), &recorded);
    pump(&[&alice, &server, &alice]);

    // SchemaAdmission: a client on a schema version the server has not
    // admitted is told so.
    let bob = open(0x52, user(2), true);
    connect(&bob, &server, user(2), &recorded);
    pump(&[&bob, &server, &bob]);

    // Stored: a worker relays its tab's write upstream.
    let worker = open(0x53, user(1), false);
    connect(&worker, &server, user(1), &recorded);
    let tab = open(0x54, user(1), false);
    tab.set_non_durable_client();
    connect(&tab, &worker, user(1), &recorded);
    let tabs = block_on(tab.insert("notes", note(1, vec![5]), InsertOptions::default()))
        .unwrap()
        .row_uuid();
    pump(&[&tab, &worker, &server, &worker, &tab]);
    // Commit with bases and an origin: the tab edits its note twice before
    // the first edit settles, and the worker relays both.
    for body in [6, 7] {
        block_on(tab.update("notes", tabs, note(1, vec![body]), UpdateOptions::default())).unwrap();
    }
    pump(&[&tab, &worker, &server, &worker, &tab]);

    // Fate rejected with a reason: alice stamps a write further ahead of
    // the server's clock than the skew tolerance allows.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    block_on(alice.insert(
        "notes",
        note(1, vec![6]),
        InsertOptions {
            updated_at_ms: Some(now + jazz::db::SKEW_TOLERANCE_MS as u64 + 60_000),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&[&alice, &server, &alice]);

    // Fate rejected without a reason: another author claims alice's node.
    let mallory = open(0x51, user(2), false);
    connect(&mallory, &server, user(2), &recorded);
    block_on(mallory.insert("notes", note(2, vec![7]), InsertOptions::default())).unwrap();
    pump(&[&mallory, &server, &mallory]);

    // Catalogue with versions and cells: the server admits a version whose
    // lens copies a column while alice is connected.
    let copied = schema_with_copy();
    let lens = MigrationLens::new(
        schema(false).version_id(),
        copied.version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "notes".into(),
            ops: vec![LensOp::CopyColumn {
                from: "owner".into(),
                to: "first_owner".into(),
            }],
        }],
    )
    .unwrap();
    block_on(server.activate_schema_with_lenses(copied, &[lens])).unwrap();
    pump(&[&alice, &server, &alice]);

    // Refused: a subscription the server cannot serve. No replica sends
    // one (it prepares queries against the same schema), so a raw session
    // does, on the schema version alice announced.
    let schema = postcard::from_bytes::<SyncMessage>(&recorded.borrow()["Schema"].1).unwrap();
    let inbound = Rc::new(RefCell::new(VecDeque::from([
        schema,
        SyncMessage::Read {
            id: 1,
            query: Box::new(Query::from("missing")),
            binding: BTreeMap::new(),
            include_deleted: false,
            tier: ReadTier::LocalFirst,
            wait: None,
            span: ReadSpan::Live { since: None },
        },
    ])));
    server.accept_subscriber(
        Box::new(Recording {
            outbound: Rc::default(),
            inbound,
            peer: "client",
            recorded: recorded.clone(),
        }),
        user(3),
    );
    pump(&[&server]);

    // Hold: a replica asks for a row whole when a shipped delta does not
    // apply over the version it holds, which takes a corrupted delta to
    // provoke; its frame is pinned as a replica encodes it.
    let hold = SyncMessage::Hold {
        rows: vec![("notes".to_owned(), [0x11; 16], Vec::new(), 0)],
    };
    let [frame] = encode_frames(hold).unwrap().try_into().unwrap();
    recorded
        .borrow_mut()
        .insert("Hold".to_owned(), ("server".to_owned(), frame));

    recorded.borrow().clone()
}

/// Writes `fixtures/sqlite_sync_wire_v1.json` when
/// `JAZZ_WRITE_SQLITE_SYNC_WIRE_CORPUS=1`. Until alpha.60 ships, re-run it
/// for a sync message layout change; after, add a new corpus instead.
#[test]
#[ignore = "#1787: manual wire corpus producer; run for a sync wire layout change"]
fn produce_sqlite_sync_wire_corpus_v1() {
    assert_eq!(
        std::env::var("JAZZ_WRITE_SQLITE_SYNC_WIRE_CORPUS").as_deref(),
        Ok("1"),
        "set JAZZ_WRITE_SQLITE_SYNC_WIRE_CORPUS=1 to overwrite the pinned corpus"
    );
    let mut recorded = record();
    let missing: Vec<&str> = VARIANTS
        .iter()
        .copied()
        .filter(|v| !recorded.contains_key(*v))
        .collect();
    assert!(missing.is_empty(), "the scenario never sent {missing:?}");
    let fates: Vec<Fate> = [Some(Reason::ClientClockTooFarAhead.as_str()), None]
        .into_iter()
        .map(|reason| {
            let name = rejected_fate_key(reason);
            let (reader, frame) = recorded
                .remove(&name)
                .unwrap_or_else(|| panic!("the scenario never sent a {name}"));
            Fate {
                name,
                reason: reason.map(str::to_owned),
                reader,
                frame_hex: hex(&frame),
            }
        })
        .collect();
    let messages: Vec<Message> = VARIANTS
        .iter()
        .map(|v| {
            let (reader, frame) = recorded.remove(*v).unwrap();
            Message {
                variant: (*v).to_owned(),
                reader,
                frame_hex: hex(&frame),
            }
        })
        .collect();

    let batch = |frames: &[Vec<u8>]| hex(&encode_websocket_frame_batch(frames).unwrap());
    let hello = |role| {
        encode_frame(&WireFrame::Hello(WireHello::current(
            role,
            current_wire_features(),
        )))
        .unwrap()
    };
    let error = encode_frame(&WireFrame::Error(WireError::new(
        WireErrorCode::MalformedFrame,
        WireRetry::Never,
        "malformed frame",
    )))
    .unwrap();
    let shapes: Vec<(&str, String, Vec<u8>)> = SHAPES
        .iter()
        .map(|name| {
            let (reader, frame) = recorded
                .remove(*name)
                .unwrap_or_else(|| panic!("the scenario never sent a {name}"));
            (*name, reader, frame)
        })
        .collect();
    let rows = shapes[1].2.clone();
    let mut commit = unhex(&messages[4].frame_hex);
    commit.truncate(commit.len() / 2);
    let subscribe = unhex(&messages[0].frame_hex);
    let unsubscribe = unhex(&messages[1].frame_hex);
    // Wire v5 hellos are refused; the frame comes from the v6 corpus.
    let v6_corpus: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/wire_frame_artifact_corpus_v6.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let v5_hello = v6_corpus["rejections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "unsupported exact-v5 hello")
        .unwrap()["frame_hex"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = |name: &str, reader: &str, batch_hex: String| Batch {
        name: name.to_owned(),
        reader: reader.to_owned(),
        batch_hex,
    };
    let corpus = Corpus {
        format: FORMAT.to_owned(),
        messages,
        fates,
        frames: vec![
            entry(
                "client hello",
                "server-opening",
                batch(&[hello(WirePeerRole::Client)]),
            ),
            entry("core hello", "client", batch(&[hello(WirePeerRole::Core)])),
            entry(
                "relay hello",
                "server-opening",
                batch(&[hello(WirePeerRole::Relay)]),
            ),
            entry(
                "two messages in one batch",
                "server",
                batch(&[subscribe.clone(), unsubscribe]),
            ),
            entry("structured error", "client", batch(&[error])),
            entry(
                "a split message in one batch",
                "client",
                batch(&split(&rows)),
            ),
        ]
        .into_iter()
        .chain(
            shapes
                .iter()
                .map(|(name, reader, frame)| entry(name, reader, batch(&[frame.clone()]))),
        )
        .collect(),
        rejections: vec![
            entry(
                "unknown sync message discriminant",
                "server",
                batch(&[vec![0x7f]]),
            ),
            entry(
                "unknown sync message discriminant",
                "client",
                batch(&[vec![0x7f]]),
            ),
            entry("truncated commit", "server", batch(&[commit])),
            entry(
                "a sync message with bytes after it",
                "server",
                batch(&[[subscribe.as_slice(), &[0]].concat()]),
            ),
            entry(
                "a client hello followed by a message in the opening batch",
                "server-opening",
                batch(&[hello(WirePeerRole::Client), subscribe.clone()]),
            ),
            entry(
                "a sync message as the opening batch",
                "server-opening",
                batch(&[subscribe.clone()]),
            ),
            entry(
                "unsupported Groove-era v5 hello",
                "server-opening",
                batch(&[unhex(&v5_hello)]),
            ),
            entry(
                "a client-bound frame with tag 0 that is not a hello",
                "client",
                batch(&[subscribe]),
            ),
            entry(
                "a client-bound frame with tag 1 that is not an error",
                "client",
                batch(&[vec![1, 0xff]]),
            ),
            entry("not a websocket frame batch", "server", hex(&[0xff, 0xff])),
            entry(
                "a split message's last part with no part before it",
                "client",
                batch(&[split(&rows)[1].clone()]),
            ),
            entry(
                "a whole message between a split message's parts",
                "client",
                batch(&{
                    let [head, tail] = split(&rows);
                    [head, rows.clone(), tail]
                }),
            ),
        ],
    };
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(CORPUS);
    std::fs::write(path, serde_json::to_string_pretty(&corpus).unwrap() + "\n").unwrap();
}

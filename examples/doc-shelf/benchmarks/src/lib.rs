//! DocShelf: a team's markdown document library. Most bodies are long
//! (80 to 240 KiB, so they are stored as chunked large values); a quarter are
//! short notes that stay inline. A quarter of the long documents start from
//! the same 6 KiB meeting-notes template, so they share a long common prefix.
//!
//! The workloads are what a writer notices: opening the shelf, searching the
//! bodies, finding a pasted duplicate, sorting by body, typing into an open
//! document, importing a long document, and a new device syncing the shelf.
//!
//! Queries never read a chunked body's content (SPEC 19.5): `contains` and
//! ordering see only the inline notes' content, a chunked body compares by
//! cell, and an equality literal over 64 KiB is refused. The workloads use the
//! shapes that still answer: searching finds the needle in inline notes,
//! duplicates are found by grouping on the body cell, and ordering puts the
//! chunked bodies first.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, ReadOpts, ReadTier, SubscriberId,
    SubscriptionEvent, SubscriptionStream, TransportError, Upstream, WireTransport,
    WireTransportAdapter, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, contains, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

/// Documents on the benchmark shelf, before the pasted duplicate.
pub const DOCS: usize = 96;
/// The phrase the search workload looks for.
pub const NEEDLE: &str = "kumquat protocol";
/// Documents whose body contains [`NEEDLE`]; only the inline ones are
/// found by `contains` ([`needle_found`]).
pub const NEEDLE_DOCS: [usize; 4] = [3, 6, 16, 41];
/// The document pasted a second time onto the shelf.
pub const PASTED: usize = 10;
/// The document open in the editor.
pub const OPEN: usize = 1;
/// The size of an imported document.
pub const IMPORT_BYTES: usize = 1024 * 1024;

const WORDS: [&str; 48] = [
    "stage",
    "light",
    "cue",
    "crew",
    "budget",
    "venue",
    "rider",
    "load",
    "truck",
    "sound",
    "check",
    "monitor",
    "mix",
    "set",
    "list",
    "encore",
    "ticket",
    "door",
    "merch",
    "tour",
    "route",
    "hotel",
    "flight",
    "bus",
    "visa",
    "contract",
    "deposit",
    "insurance",
    "permit",
    "power",
    "rigging",
    "truss",
    "haze",
    "laser",
    "screen",
    "camera",
    "stream",
    "press",
    "poster",
    "radio",
    "promoter",
    "agent",
    "manager",
    "catering",
    "parking",
    "security",
    "barrier",
    "curfew",
];

/// Deterministic markdown that never repeats, so the chunker cuts it into
/// many content-defined leaves.
pub fn markdown(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut out = String::with_capacity(len + 64);
    while out.len() < len {
        match next() % 24 {
            0 => out.push_str("\n\n## "),
            1 => out.push_str("\n- "),
            2 => out.push_str(".\n"),
            _ => out.push(' '),
        }
        out.push_str(WORDS[(next() % WORDS.len() as u64) as usize]);
        if next() % 9 == 0 {
            out.push_str(&format!(" {}", next() % 1000));
        }
    }
    out.truncate(len);
    out
}

/// The 6 KiB meeting-notes template a quarter of the long documents start
/// with. It sorts before every other body.
pub fn template() -> String {
    let mut head = String::from("# 0 Weekly production sync\n\n");
    head.push_str(&markdown(9_999, 6 * 1024 - head.len()));
    head
}

/// The body of shelf document `n`.
pub fn body(n: usize) -> String {
    let mut body = if n % 4 == 3 {
        format!("# Note {n}\n{}", markdown(n as u64 + 1, 3 * 1024))
    } else {
        let len = (80 + (n * 7919) % 160) * 1024;
        let head = if n.is_multiple_of(4) {
            template()
        } else {
            format!("# Doc {n}\n")
        };
        format!("{head}{}", markdown(n as u64 + 1, len - head.len()))
    };
    if NEEDLE_DOCS.contains(&n) {
        // Near the end, in the middle, and near the start of different bodies.
        let at = match n % 3 {
            0 => body.len() - 100,
            1 => body.len() / 2,
            _ => 40,
        };
        body.insert_str(at, NEEDLE);
    }
    body
}

/// Whether shelf document `n`'s body is chunked: over the 64 KiB inline
/// threshold.
pub fn chunked(n: usize) -> bool {
    body(n).len() > jazz::large_values::INLINE_VALUE_MAX_BYTES
}

/// The documents `contains(body, NEEDLE)` finds: those holding the needle in
/// an inline body.
pub fn needle_found() -> Vec<usize> {
    NEEDLE_DOCS.into_iter().filter(|&n| !chunked(n)).collect()
}

pub fn doc_id(n: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0xd0;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("folder", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column("updated_at", ColumnType::Integer)
                    .index_only(["title", "body"]),
            )
            .build()
            .allow_all(),
    )
    .expect("DocShelf schema compiles")
}

/// The DocShelf schema for a node.
pub fn config(node: u8) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// Inserts a document and waits until it is locally durable.
pub fn insert(db: &Db, id: RowUuid, title: String, body: String, updated_at: i32) {
    let write = block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("title".to_owned(), Value::String(title)),
            ("folder".to_owned(), Value::String("Tour 2026".to_owned())),
            ("body".to_owned(), Value::String(body)),
            ("updated_at".to_owned(), Value::I32(updated_at)),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert document");
    block_on(write.wait(DurabilityTier::Local)).expect("document durable");
}

/// Fills `db` with the shelf, plus a second copy of document [`PASTED`].
pub fn seed(db: &Db) {
    for n in 0..DOCS {
        insert(db, doc_id(n), format!("Doc {n:03}"), body(n), n as i32);
    }
    insert(
        db,
        doc_id(DOCS),
        format!("Doc {PASTED:03} (copy)"),
        body(PASTED),
        DOCS as i32,
    );
}

/// A seeded shelf in memory with its prepared reads.
pub struct Fixture {
    db: Db,
    list: PreparedQuery,
    search: PreparedQuery,
    duplicate: PreparedQuery,
    by_body: PreparedQuery,
    editor: PreparedQuery,
    imported: Cell<usize>,
}

/// The open document's live editor view.
pub struct Editor {
    view: SubscriptionStream,
}

impl Fixture {
    pub fn new() -> Self {
        let db = block_on(Db::open(config(0x51))).expect("open DocShelf database");
        seed(&db);
        let prepare = |query: Query| db.prepare_query(&query).expect("prepare DocShelf query");
        let list = prepare(
            Query::from("docs")
                .order_by("title", OrderDirection::Asc)
                .limit(50)
                .select(["title", "folder", "updated_at"]),
        );
        let search = prepare(
            Query::from("docs")
                .filter(contains(col("body"), lit(Value::String(NEEDLE.to_owned()))))
                .select(["title"]),
        );
        // Two bodies written whole with the same content share one cell, so
        // grouping by the cell finds the pasted copy without reading bodies.
        let duplicate = prepare(
            Query::from("docs")
                .count()
                .group_by("body")
                .order_by("count", OrderDirection::Desc)
                .limit(1),
        );
        let by_body = prepare(
            Query::from("docs")
                .order_by("body", OrderDirection::Asc)
                .limit(20)
                .select(["title"]),
        );
        let editor = prepare(Query::from("docs").filter(eq(col("id"), lit(doc_id(OPEN).0))));
        Self {
            list,
            search,
            duplicate,
            by_body,
            editor,
            db,
            imported: Cell::new(0),
        }
    }

    /// Opening the shelf: subscribe to the first 50 titles and receive them.
    pub fn open_shelf(&self) -> usize {
        initial_rows(&mut self.subscribe(&self.list))
    }

    /// Searching every body for a phrase.
    pub fn search(&self) -> Vec<String> {
        self.titles(&self.search)
    }

    /// Finding the most repeated body: its row count and its content.
    pub fn find_duplicates(&self) -> (u64, String) {
        let rows =
            block_on(self.db.all(&self.duplicate, ReadOpts::default())).expect("read duplicates");
        let count = match rows[0].cell("count") {
            Some(Value::U64(count)) => count,
            other => panic!("unexpected count cell: {other:?}"),
        };
        match rows[0].cell("body") {
            Some(Value::String(body)) => (count, body),
            other => panic!("unexpected body cell: {other:?}"),
        }
    }

    /// Looking a long document up by its full text, which queries refuse:
    /// the error message.
    pub fn find_by_long_literal(&self) -> String {
        let query = Query::from("docs")
            .filter(eq(col("body"), lit(Value::String(body(PASTED)))))
            .select(["title"]);
        self.db
            .prepare_query(&query)
            .and_then(|prepared| block_on(self.db.all(&prepared, ReadOpts::default())))
            .map(|_| ())
            .expect_err("a literal over 64 KiB is refused")
            .message
    }

    /// The first 20 titles ordered by body: chunked bodies sort first.
    pub fn first_by_body(&self) -> Vec<String> {
        self.titles(&self.by_body)
    }

    /// Opening a long document in the editor.
    pub fn editor(&self) -> Editor {
        let mut view = self.subscribe(&self.editor);
        assert_eq!(initial_rows(&mut view), 1);
        Editor { view }
    }

    /// Typing a word at the end of the open document, until the editor
    /// shows it. Returns the rows the editor view updated.
    pub fn type_word(&self, editor: &mut Editor) -> usize {
        block_on(
            self.db
                .append_value("docs", doc_id(OPEN), "body", b" encore".to_vec()),
        )
        .expect("append to the open document");
        match block_on(editor.view.next_event()).expect("editor observes the keystroke") {
            SubscriptionEvent::Delta { updated, .. } => updated.len(),
            event => panic!("unexpected editor event: {event:?}"),
        }
    }

    /// Importing one long document. Returns the bytes imported.
    pub fn import(&self) -> usize {
        let n = self.imported.get();
        self.imported.set(n + 1);
        let body = markdown(1_000_000 + n as u64, IMPORT_BYTES);
        let len = body.len();
        insert(
            &self.db,
            doc_id(10_000 + n),
            format!("Import {n:05}"),
            body,
            0,
        );
        len
    }

    /// The open document's body as stored.
    pub fn open_body(&self) -> String {
        let rows = block_on(self.db.all(&self.editor, ReadOpts::default())).expect("read body");
        match rows[0]
            .cells()
            .find(|(name, _)| *name == "body")
            .and_then(|(_, value)| value.cloned())
        {
            Some(Value::String(body)) => body,
            other => panic!("unexpected body cell: {other:?}"),
        }
    }

    fn titles(&self, query: &PreparedQuery) -> Vec<String> {
        block_on(self.db.all(query, ReadOpts::default()))
            .expect("read DocShelf query")
            .iter()
            .map(|row| {
                match row
                    .cells()
                    .find(|(name, _)| *name == "title")
                    .and_then(|(_, value)| value.cloned())
                {
                    Some(Value::String(title)) => title,
                    other => panic!("unexpected title cell: {other:?}"),
                }
            })
            .collect()
    }

    fn subscribe(&self, query: &PreparedQuery) -> SubscriptionStream {
        block_on(self.db.subscribe(query, ReadOpts::default())).expect("subscribe DocShelf query")
    }
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

fn initial_rows(stream: &mut SubscriptionStream) -> usize {
    match block_on(stream.next_event()).expect("subscription has an initial result") {
        SubscriptionEvent::Delta {
            reset: true, added, ..
        } => added.len(),
        event => panic!("unexpected initial DocShelf event: {event:?}"),
    }
}

/// A seeded authority a new device syncs the whole shelf from.
pub struct SyncFixture {
    server: Db,
    devices: Cell<u8>,
}

/// A device that has just connected to the authority.
pub struct Device {
    db: Db,
    stream: SubscriptionStream,
    sent: Rc<RefCell<usize>>,
    _link: (Upstream, SubscriberId),
}

impl SyncFixture {
    pub fn new() -> Self {
        let server = block_on(Db::open(config(0x52))).expect("open DocShelf authority");
        seed(&server);
        Self {
            server,
            devices: Cell::new(0),
        }
    }

    /// A fresh device, connected and subscribed to the whole shelf.
    pub fn device(&self) -> Device {
        let node = self.devices.get();
        self.devices.set(node.wrapping_add(1));
        let db = block_on(Db::open(config(0x80 | (node & 0x7f)))).expect("open device");
        let (client_side, server_side, sent) = byte_duplex();
        let upstream = block_on(db.connect_upstream(client_side));
        let subscriber = self
            .server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM);
        let prepared = db
            .prepare_query(&Query::from("docs"))
            .expect("prepare shelf");
        let stream = block_on(db.subscribe(
            &prepared,
            ReadOpts {
                tier: ReadTier::Remote,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe shelf");
        Device {
            db,
            stream,
            sent,
            _link: (upstream, subscriber),
        }
    }

    /// Syncing until the device shows every document with its whole body.
    /// Returns the documents shown, the round trips taken and the bytes the
    /// authority sent.
    pub fn sync(&self, mut device: Device) -> (usize, usize, usize) {
        let mut rows = BTreeMap::new();
        let mut rounds = 0;
        while rounds < 4096 {
            rounds += 1;
            block_on(device.db.tick()).expect("device tick");
            block_on(self.server.tick()).expect("authority tick");
            block_on(device.db.tick()).expect("device tick");
            while let Some(event) = device.stream.try_next_event() {
                if let SubscriptionEvent::Delta {
                    reset,
                    added,
                    updated,
                    removed,
                    ..
                } = event
                {
                    if reset {
                        rows.clear();
                    }
                    for row in added.iter().chain(updated.iter()) {
                        let len = row
                            .cells()
                            .find(|(name, _)| *name == "body")
                            .and_then(|(_, value)| value.cloned())
                            .map(|value| match value {
                                Value::String(body) => body.len(),
                                other => panic!("unexpected body cell: {other:?}"),
                            })
                            .unwrap_or(0);
                        rows.insert(row.row_uuid(), len);
                    }
                    for id in removed.iter() {
                        rows.remove(&id.row_uuid);
                    }
                }
            }
            if rows.len() == DOCS + 1 {
                break;
            }
        }
        let sent = *device.sent.borrow();
        (rows.len(), rounds, sent)
    }
}

impl Default for SyncFixture {
    fn default() -> Self {
        Self::new()
    }
}

struct ByteDuplexTransport {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    sent: Option<Rc<RefCell<usize>>>,
}

impl WireTransport for ByteDuplexTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        if let Some(sent) = &self.sent {
            *sent.borrow_mut() += frame.len();
        }
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

type ByteLink = (
    Box<dyn jazz::db::Transport>,
    Box<dyn jazz::db::Transport>,
    Rc<RefCell<usize>>,
);

fn byte_duplex() -> ByteLink {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let sent = Rc::new(RefCell::new(0));
    let client = ByteDuplexTransport {
        outbound: Rc::clone(&up),
        inbound: Rc::clone(&down),
        sent: None,
    };
    let server = ByteDuplexTransport {
        outbound: down,
        inbound: up,
        sent: Some(Rc::clone(&sent)),
    };
    (
        Box::new(WireTransportAdapter::current(client)),
        Box::new(WireTransportAdapter::current(server)),
        sent,
    )
}

/// Seeds a file store at `path` and returns its size on disk in bytes.
pub fn file_footprint(path: &std::path::Path) -> u64 {
    {
        let db = block_on(Db::open_at(
            config(0x53),
            path,
            jazz::db::FileOptions::default(),
        ))
        .expect("open DocShelf file store");
        seed(&db);
    }
    let dir = path.parent().expect("store has a directory");
    std::fs::read_dir(dir)
        .expect("list store directory")
        .map(|entry| entry.expect("store entry").metadata().expect("size").len())
        .sum()
}

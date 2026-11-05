//! Typing into a text-merge column: keystrokes written as splices cost a
//! few bytes on the wire and in history, at every store, once the document
//! is over the column's 4 KiB inline threshold; and when two people type at
//! once, the authority's merges travel as edits too.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, LargeValueUpdateSplice, ReadOpts,
    ReadTier, SubscriptionStream, UpdateOptions, WireTransportAdapter, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::large_values::{INLINE_VALUE_MAX_BYTES, TEXT_MERGE_INLINE_MAX_BYTES};
use jazz::query::{Query, col, contains, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Schema, TableName,
    TableSchema,
};
use jazz::wire::{TransportError, WireTransport};

use crate::common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(&Schema::from([(
        TableName::new("docs"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("body", ColumnType::Text)
                    .merge_strategy(ColumnMergeStrategy::Markdown),
                ColumnDescriptor::new("plain", ColumnType::Text),
            ]),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: allow_all_policies(),
            branch_by: Vec::new(),
        },
    )]))
}

fn config(node: u8) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn row() -> RowUuid {
    RowUuid::from_bytes([0xd0; 16])
}

/// Deterministic markdown-ish text that does not repeat.
fn text(len: usize, seed: u64) -> String {
    let mut state = seed | 1;
    let mut out = String::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push(match state % 9 {
            0 => ' ',
            1 => '\n',
            n => char::from(b'a' + (n as u8) * 3),
        });
    }
    out
}

/// A link that counts the bytes each side sends.
#[derive(Clone, Default)]
struct Counter(Rc<RefCell<usize>>);

struct Counted {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    sent: Counter,
}

impl WireTransport for Counted {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        *self.sent.0.borrow_mut() += frame.len();
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// A device connected to `server`, subscribed to the document.
struct Device {
    db: Db,
    up: Counter,
    down: Counter,
    stream: SubscriptionStream,
    _link: Box<dyn std::any::Any>,
}

fn connect(db: Db, server: &Db) -> Device {
    let (to_server, to_device) = (Rc::default(), Rc::default());
    let (up, down) = (Counter::default(), Counter::default());
    let device = Counted {
        outbound: Rc::clone(&to_server),
        inbound: Rc::clone(&to_device),
        sent: up.clone(),
    };
    let authority = Counted {
        outbound: to_device,
        inbound: to_server,
        sent: down.clone(),
    };
    let upstream = block_on(db.connect_upstream(Box::new(WireTransportAdapter::current(device))));
    let subscriber = server
        .accept_subscriber(
            Box::new(WireTransportAdapter::current(authority)),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    let query = db
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(row().0))))
        .unwrap();
    let remote = ReadOpts {
        tier: ReadTier::Remote,
        ..ReadOpts::default()
    };
    let stream = block_on(db.subscribe(&query, remote)).unwrap();
    Device {
        db,
        up,
        down,
        stream,
        _link: Box::new((upstream, subscriber)),
    }
}

/// Tick everyone until no link carries anything for three rounds.
fn settle(server: &Db, devices: &mut [&mut Device]) {
    let total = |devices: &[&mut Device]| {
        devices
            .iter()
            .map(|d| *d.up.0.borrow() + *d.down.0.borrow())
            .sum::<usize>()
    };
    let mut quiet = 0;
    let mut rounds = 0;
    while quiet < 3 {
        rounds += 1;
        assert!(rounds < 2000, "sync settles");
        let before = total(devices);
        for device in devices.iter() {
            block_on(device.db.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
        for device in devices.iter_mut() {
            block_on(device.db.tick()).unwrap();
            while device.stream.try_next_event().is_some() {}
        }
        quiet = if total(devices) == before {
            quiet + 1
        } else {
            0
        };
    }
}

fn body(db: &Db) -> String {
    let query = db
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(row().0))))
        .unwrap();
    let rows = block_on(db.all(&query, ReadOpts::default())).unwrap();
    match rows[0].cell("body") {
        Some(Value::String(body)) => body,
        other => panic!("unexpected body: {other:?}"),
    }
}

fn insert(db: &Db, body: &str) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("body".to_owned(), Value::String(body.to_owned())),
            ("plain".to_owned(), Value::String(String::new())),
        ]),
        InsertOptions {
            row_id: Some(row()),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn splice(db: &Db, at: usize, insert: &str) {
    block_on(db.splice_value(
        "docs",
        row(),
        "body",
        LargeValueUpdateSplice {
            at: at as u64,
            delete: 0,
            insert: insert.as_bytes().to_vec(),
        },
        UpdateOptions::default(),
    ))
    .unwrap();
}

/// The document written whole, as an editor binding does on each change.
fn write_whole(db: &Db, body: &str) {
    block_on(db.update(
        "docs",
        row(),
        BTreeMap::from([("body".to_owned(), Value::String(body.to_owned()))]),
        UpdateOptions::default(),
    ))
    .unwrap();
}

fn store_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum()
}

/// A writer and an observer over an authority, all file stores, after the
/// writer types `keys` characters into a document of `len` bytes: the bytes
/// on each link per keystroke, and each store's size once closed.
fn type_into(len: usize, keys: usize, whole: bool) -> (f64, f64, Vec<u64>) {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let open = |i: usize, node: u8| {
        block_on(Db::open_at(
            config(node),
            &dirs[i].path().join("store.sqlite"),
            FileOptions::default(),
        ))
        .unwrap()
    };
    let server = open(0, 0x50);
    let mut writer = connect(open(1, 0x81), &server);
    let mut observer = connect(open(2, 0x82), &server);
    let mut expected = text(len, 7);
    insert(&writer.db, &expected);
    settle(&server, &mut [&mut writer, &mut observer]);
    let (up, down) = (*writer.up.0.borrow(), *observer.down.0.borrow());
    let mut cursor = len / 2;
    for key in 0..keys {
        let typed = if key % 6 == 5 { " " } else { "k" };
        expected.insert_str(cursor, typed);
        if whole {
            write_whole(&writer.db, &expected);
        } else {
            splice(&writer.db, cursor, typed);
        }
        cursor += 1;
        settle(&server, &mut [&mut writer, &mut observer]);
    }
    assert_eq!(body(&observer.db), expected);
    assert_eq!(body(&server), expected);
    let per_key = |now: usize, then: usize| (now - then) as f64 / keys.max(1) as f64;
    let wire = (
        per_key(*writer.up.0.borrow(), up),
        per_key(*observer.down.0.borrow(), down),
    );
    drop((writer, observer, server));
    let stores = dirs.iter().map(|dir| store_bytes(dir.path())).collect();
    (wire.0, wire.1, stores)
}

/// A keystroke into a 12 KiB markdown document costs a few hundred bytes
/// on each link and well under that in each store's history, whether it is
/// written as a splice or the document is written whole (as markdownSync
/// does): a whole write over a chunked value is stored as the splice that
/// makes it.
#[test]
fn a_keystroke_in_a_markdown_document_costs_bytes_not_the_document() {
    const LEN: usize = 12 * 1024;
    for whole in [false, true] {
        let (_, _, before) = type_into(LEN, 0, whole);
        let (up, down, after) = type_into(LEN, 100, whole);
        assert!(
            up < 512.0,
            "writer sends {up} bytes per keystroke (whole: {whole})"
        );
        assert!(
            down < 1024.0,
            "observer receives {down} bytes per keystroke (whole: {whole})"
        );
        for (store, (then, now)) in ["authority", "writer", "observer"]
            .iter()
            .zip(before.iter().zip(&after))
        {
            let per_key = (*now as f64 - *then as f64) / 100.0;
            assert!(
                per_key < 512.0,
                "{store} keeps {per_key} bytes per keystroke (whole: {whole})"
            );
        }
    }
}

/// Two people typing into one document at once: every write merges on the
/// authority, and each merge goes back out as an edit of the held value,
/// not the merged document whole; with splices or whole writes alike.
#[test]
fn two_people_typing_at_once_merge_as_edits() {
    const LEN: usize = 48 * 1024;
    const KEYS: usize = 40;
    for whole in [false, true] {
        let server = block_on(Db::open(config(0x50))).unwrap();
        let mut a = connect(block_on(Db::open(config(0x81))).unwrap(), &server);
        let mut b = connect(block_on(Db::open(config(0x82))).unwrap(), &server);
        let start = text(LEN, 11);
        insert(&a.db, &start);
        settle(&server, &mut [&mut a, &mut b]);
        let down = *a.down.0.borrow() + *b.down.0.borrow();
        let up = *a.up.0.borrow() + *b.up.0.borrow();
        let type_at = |db: &Db, at: usize, typed: &str| {
            if whole {
                let mut body = body(db);
                body.insert_str(at, typed);
                write_whole(db, &body);
            } else {
                splice(db, at, typed);
            }
        };
        for key in 0..KEYS {
            // Each types before the other's last keystroke arrives.
            type_at(&a.db, 10 + key, "a");
            type_at(&b.db, LEN + 2 * key, "b");
            settle(&server, &mut [&mut a, &mut b]);
        }
        let merged = body(&server);
        assert_eq!(body(&a.db), merged);
        assert_eq!(body(&b.db), merged);
        assert_eq!(
            merged.matches('a').count(),
            start.matches('a').count() + KEYS
        );
        assert_eq!(
            merged.matches('b').count(),
            start.matches('b').count() + KEYS
        );
        let keys = (2 * KEYS) as f64;
        let up = (*a.up.0.borrow() + *b.up.0.borrow() - up) as f64 / keys;
        let down = (*a.down.0.borrow() + *b.down.0.borrow() - down) as f64 / keys;
        assert!(
            up < 1024.0,
            "each keystroke sends {up} bytes up (whole: {whole})"
        );
        assert!(
            down < 8.0 * 1024.0,
            "each keystroke brings {down} bytes down (whole: {whole})"
        );
    }
}

/// A text-merge column chunks values over 4 KiB, which queries no longer
/// read (SPEC 19.5); a plain text column keeps the 64 KiB threshold.
#[test]
fn text_merge_columns_chunk_over_four_kib() {
    let db = block_on(Db::open(config(0x51))).unwrap();
    let long = format!("{} needle", text(TEXT_MERGE_INLINE_MAX_BYTES, 3));
    block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("body".to_owned(), Value::String(long.clone())),
            ("plain".to_owned(), Value::String(long.clone())),
        ]),
        InsertOptions::default(),
    ))
    .unwrap();
    let found = |column: &str| {
        let query = db
            .prepare_query(
                &Query::from("docs")
                    .filter(contains(col(column), lit(Value::String("needle".into())))),
            )
            .unwrap();
        block_on(db.all(&query, ReadOpts::default())).unwrap().len()
    };
    assert_eq!(found("plain"), 1, "an inline plain value is searched");
    assert_eq!(found("body"), 0, "a chunked markdown value is not");
    let literal = |column: &str| {
        db.prepare_query(
            &Query::from("docs").filter(eq(col(column), lit(Value::String(long.clone())))),
        )
        .and_then(|query| block_on(db.all(&query, ReadOpts::default())))
    };
    assert_eq!(literal("plain").unwrap().len(), 1);
    let refused = literal("body").unwrap_err();
    assert!(refused.message.contains("not supported yet"), "{refused}");
    assert!(
        refused
            .message
            .contains(&TEXT_MERGE_INLINE_MAX_BYTES.to_string()),
        "{refused}"
    );
    assert!(INLINE_VALUE_MAX_BYTES > long.len());
}

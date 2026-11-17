//! Typing into a text-merge column: keystrokes written as splices cost a
//! few bytes on the wire and in history, at every store, once the document
//! is over the column's 4 KiB inline threshold; and when two people type at
//! once, the authority's merges travel as edits too.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, LargeValueUpdate, LargeValueUpdateSplice,
    ReadOpts, ReadTier, SubscriptionStream, UpdateOptions, WireTransportAdapter, block_on,
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
            history: Default::default(),
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
    let subscriber = jazz::db::block_on(server.accept_subscriber(
        Box::new(WireTransportAdapter::current(authority)),
        AuthorSubject::SYSTEM,
    ))
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

fn nodes(db: &Db) -> (u64, u64) {
    jazz::db::block_on(db.large_value_nodes_for_test()).unwrap()
}

fn open_file(dir: &Path, node: u8) -> Db {
    block_on(Db::open_at(
        config(node),
        &dir.join("store.sqlite"),
        FileOptions::default(),
    ))
    .unwrap()
}

/// Multibyte text keeps its characters whole: a whole write that adds or
/// removes characters of every width next to others is stored as the
/// splice between them, on character boundaries, and reads back exactly.
#[test]
fn whole_writes_of_multibyte_text_splice_on_character_boundaries() {
    let db = block_on(Db::open(config(0x52))).unwrap();
    let mut expected = "aé€🙂".repeat(1200);
    insert(&db, &expected);
    let (stored, roots) = nodes(&db);
    assert_eq!(roots, 1);
    // Each edit shares a prefix and suffix that end inside a character of
    // the other value's bytes.
    for (at, typed) in [(4, "é"), (9, "€"), (13, "🙂"), (20, "e"), (27, "€é")] {
        let at = expected.char_indices().nth(at).unwrap().0;
        expected.insert_str(at, typed);
        write_whole(&db, &expected);
        assert_eq!(body(&db), expected);
    }
    let at = expected.char_indices().nth(30).unwrap().0;
    let next = expected[at..].chars().next().unwrap().len_utf8();
    expected.replace_range(at..at + next, "");
    write_whole(&db, &expected);
    assert_eq!(body(&db), expected);
    assert_eq!(nodes(&db), (stored, 1), "edits share the value's one tree");
}

/// Writing the document whole without a change keeps the held value: no
/// new tree, no new edit.
#[test]
fn an_unchanged_whole_write_keeps_the_held_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_file(dir.path(), 0x53);
    let doc = text(12 * 1024, 5);
    insert(&db, &doc);
    let held = nodes(&db);
    let size = store_bytes(dir.path());
    for _ in 0..20 {
        write_whole(&db, &doc);
    }
    assert_eq!(nodes(&db), held);
    assert_eq!(body(&db), doc);
    drop(db);
    let grown = store_bytes(dir.path()).saturating_sub(size);
    assert!(
        grown < 20 * 1024,
        "twenty unchanged writes grew the store {grown} bytes"
    );
}

/// A document that grows past 4 KiB is chunked and stops matching
/// `contains`; shrinking under it again makes it inline and searchable.
#[test]
fn whole_writes_cross_the_threshold_both_ways() {
    let db = block_on(Db::open(config(0x54))).unwrap();
    let found = || {
        let query = db
            .prepare_query(
                &Query::from("docs")
                    .filter(contains(col("body"), lit(Value::String("needle".into())))),
            )
            .unwrap();
        block_on(db.all(&query, ReadOpts::default())).unwrap().len()
    };
    let small = format!("{} needle", text(2 * 1024, 9));
    insert(&db, &small);
    assert_eq!((found(), nodes(&db).1), (1, 0), "inline under 4 KiB");
    let large = format!("{small}{}", text(4 * 1024, 10));
    write_whole(&db, &large);
    assert_eq!(body(&db), large);
    assert_eq!((found(), nodes(&db).1), (0, 1), "chunked over 4 KiB");
    let edited = format!("{large}!");
    write_whole(&db, &edited);
    assert_eq!(body(&db), edited);
    assert_eq!(nodes(&db).1, 1, "an edit of the chunked value");
    write_whole(&db, &small);
    assert_eq!(body(&db), small);
    // History still names the chunked versions; the current one is inline.
    assert_eq!(found(), 1, "inline again");
}

/// Whole writes in a transaction are edits of what the transaction sees:
/// its own earlier writes, never another open transaction's. Here B
/// rewrites most of the document (a new tree its batch alone holds) and is
/// abandoned, while A, open at the same time, edits the committed value and
/// commits; A's row names only nodes the store keeps, across a restart.
#[test]
fn transaction_whole_writes_edit_what_the_transaction_sees() {
    use jazz::tools::OpenTransactionId;
    let dir = tempfile::tempdir().unwrap();
    let db = open_file(dir.path(), 0x55);
    let doc = text(12 * 1024, 13);
    insert(&db, &doc);

    let update = |tx: OpenTransactionId, body: &str| {
        block_on(db.tx_ref(tx).update(
            "docs",
            row(),
            BTreeMap::from([("body".to_owned(), Value::String(body.to_owned()))]),
            UpdateOptions::default(),
        ))
        .unwrap();
    };

    // An abandoned transaction leaves the value as it was.
    let rolled = OpenTransactionId::new();
    block_on(db.begin_mergeable(rolled)).unwrap();
    update(rolled, &format!("x{doc}"));
    db.abandon_transaction_handle(rolled).unwrap();
    assert_eq!(body(&db), doc);

    let a = OpenTransactionId::new();
    let b = OpenTransactionId::new();
    block_on(db.begin_mergeable(a)).unwrap();
    block_on(db.begin_mergeable(b)).unwrap();
    let mut rewritten = doc.clone();
    rewritten.insert_str(6 * 1024, &text(2 * 1024, 14));
    update(b, &rewritten);
    let mut first = doc.clone();
    first.insert(100, 'a');
    update(a, &first);
    // A second write in A edits A's first.
    let mut second = first.clone();
    second.insert_str(9000, "aa");
    update(a, &second);
    db.abandon_transaction_handle(b).unwrap();
    block_on(db.commit_mergeable_handle(a)).unwrap();
    assert_eq!(body(&db), second);
    assert_eq!(nodes(&db).1, 1);

    drop(db);
    let db = open_file(dir.path(), 0x55);
    assert_eq!(body(&db), second, "the committed value survives a restart");
    let mut third = second.clone();
    third.insert_str(50, "after restart");
    write_whole(&db, &third);
    assert_eq!(body(&db), third);
    assert_eq!(nodes(&db).1, 1);
}

/// Keystrokes on a file store, closed and reopened between them: the
/// reopened store edits the value it held.
#[test]
fn whole_writes_edit_the_held_value_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut expected = text(12 * 1024, 17);
    {
        let db = open_file(dir.path(), 0x56);
        insert(&db, &expected);
    }
    let held = {
        let db = open_file(dir.path(), 0x56);
        nodes(&db)
    };
    for key in 0..5 {
        let db = open_file(dir.path(), 0x56);
        expected.insert(4000 + key, 'k');
        write_whole(&db, &expected);
        assert_eq!(body(&db), expected);
        assert_eq!(nodes(&db), held, "an edit adds no nodes");
    }
    let db = open_file(dir.path(), 0x56);
    assert_eq!(body(&db), expected);
}

/// A merge whose result inserts more than an epoch edit holds into the held
/// value (here a 1 KiB paste merged over a concurrent keystroke) is written
/// whole, and every store converges on it; the keystrokes after it are edits
/// again.
#[test]
fn a_merge_inserting_more_than_an_epoch_edit_is_written_whole() {
    const LEN: usize = 12 * 1024;
    let server = block_on(Db::open(config(0x58))).unwrap();
    let mut a = connect(block_on(Db::open(config(0x85))).unwrap(), &server);
    let mut b = connect(block_on(Db::open(config(0x86))).unwrap(), &server);
    let start = text(LEN, 23);
    insert(&a.db, &start);
    settle(&server, &mut [&mut a, &mut b]);
    let edit = |db: &Db, at: usize, typed: &str| {
        let mut body = body(db);
        body.insert_str(at, typed);
        write_whole(db, &body);
    };
    let paste = "P".repeat(1024);
    // A's keystroke reaches the authority first; B's paste, written over
    // the value before it, merges over A's.
    edit(&a.db, 10, "a");
    edit(&b.db, LEN - 10, &paste);
    settle(&server, &mut [&mut a, &mut b]);
    let mut expected = start.clone();
    expected.insert_str(LEN - 10, &paste);
    expected.insert(10, 'a');
    assert_eq!(body(&server), expected);
    assert_eq!(body(&a.db), expected);
    assert_eq!(body(&b.db), expected);
    let down = *a.down.0.borrow();
    for key in 0..10 {
        edit(&a.db, 20 + key, "k");
        expected.insert(20 + key, 'k');
        settle(&server, &mut [&mut a, &mut b]);
    }
    assert_eq!(body(&b.db), expected);
    let per_key = (*a.down.0.borrow() - down) as f64 / 10.0;
    assert!(
        per_key < 1024.0,
        "{per_key} bytes per keystroke after the merge"
    );
}

/// An editor over the column, as markdownSync and a controlled textarea
/// keep one: its own text and cursor, brought to the column's value only
/// when its subscription delivers it (`refresh`, the next render), and the
/// value it last wrote or showed, which each write says it was made from.
struct Editor {
    text: String,
    cursor: usize,
    seen: String,
}

impl Editor {
    fn new(text: &str, cursor: usize) -> Self {
        Self {
            text: text.to_owned(),
            cursor,
            seen: text.to_owned(),
        }
    }

    /// Show `value`, the cursor mapped through the change: kept before
    /// it, moved with the text after it.
    fn refresh(&mut self, value: String) {
        let old = &self.text;
        let prefix = old
            .bytes()
            .zip(value.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        let room = old.len().min(value.len()) - prefix;
        let suffix = old
            .bytes()
            .rev()
            .zip(value.bytes().rev())
            .take(room)
            .take_while(|(a, b)| a == b)
            .count();
        if self.cursor > prefix {
            self.cursor = if self.cursor >= old.len() - suffix {
                self.cursor + value.len() - old.len()
            } else {
                value.len() - suffix
            };
        }
        self.seen = value.clone();
        self.text = value;
    }

    fn type_char(&mut self, db: &Db, c: char) {
        self.text.insert(self.cursor, c);
        self.cursor += c.len_utf8();
        block_on(db.update_with_large_value_mutations(
            "docs",
            row(),
            BTreeMap::new(),
            vec![LargeValueUpdate::TextEdit {
                column: "body".to_owned(),
                text: self.text.clone(),
                edited_from: self.seen.clone(),
            }],
            UpdateOptions::default(),
        ))
        .unwrap();
        self.seen = self.text.clone();
    }
}

/// Two editors typing at once, a keystroke each per round; with `lag`,
/// each shows the column's merged value only every other round, so it
/// types over text it has not shown yet (its store has).
fn type_together(start: &str, a: (usize, &str), b: (usize, &str), lag: bool) -> String {
    let server = block_on(Db::open(config(0x60))).unwrap();
    let mut da = connect(block_on(Db::open(config(0x91))).unwrap(), &server);
    let mut db = connect(block_on(Db::open(config(0x92))).unwrap(), &server);
    insert(&da.db, start);
    settle(&server, &mut [&mut da, &mut db]);
    let mut ea = Editor::new(start, a.0);
    let mut eb = Editor::new(start, b.0);
    let (ac, bc): (Vec<char>, Vec<char>) = (a.1.chars().collect(), b.1.chars().collect());
    for i in 0..ac.len().max(bc.len()) {
        if let Some(&c) = ac.get(i) {
            ea.type_char(&da.db, c);
        }
        if let Some(&c) = bc.get(i) {
            eb.type_char(&db.db, c);
        }
        settle(&server, &mut [&mut da, &mut db]);
        if !lag || i % 2 == 1 {
            ea.refresh(body(&da.db));
            eb.refresh(body(&db.db));
        }
    }
    settle(&server, &mut [&mut da, &mut db]);
    ea.refresh(body(&da.db));
    eb.refresh(body(&db.db));
    let merged = body(&server);
    assert_eq!(ea.text, merged);
    assert_eq!(eb.text, merged);
    merged
}

/// Alice types at the end of one paragraph while Bob types at the start of
/// the next, both a keystroke at a time; whether their editors show each
/// other's text before the next keystroke or after it, both sentences
/// stay whole.
#[test]
fn two_people_typing_next_to_each_other_keep_every_character() {
    // Short (inline) and over the 4 KiB threshold (chunked).
    for intro in [String::new(), format!("{}\n\n", text(6 * 1024, 29))] {
        let start = format!("{intro}Start here.\n\nSecond paragraph.");
        let (a, b) = (intro.len() + 11, intro.len() + 13);
        for lag in [false, true] {
            assert_eq!(
                type_together(&start, (a, " Alice was here."), (b, "Bob says hi. "), lag),
                format!("{intro}Start here. Alice was here.\n\nBob says hi. Second paragraph."),
                "lag: {lag}, {} bytes",
                start.len()
            );
        }
    }
}

/// Two people typing at the very same spot both keep every character.
/// When each sees the other's keystrokes before typing the next, each word
/// stays whole; typing over keystrokes not yet shown, the letters
/// interleave in the order the authority sequenced them.
#[test]
fn two_people_typing_at_one_spot_keep_every_character() {
    let start = "Start here.\n\nSecond paragraph.";
    for (lag, expected) in [
        (false, "Start here.AAAA BBBB \n\nSecond paragraph."),
        (true, "Start here.ABABABAB \n\nSecond paragraph."),
    ] {
        assert_eq!(
            type_together(start, (11, "AAAA "), (11, "BBBB "), lag),
            expected,
            "lag: {lag}"
        );
    }
}

/// Someone deletes what this editor just typed and goes on typing; the
/// editor, showing the value from before its keystroke again, types into
/// it. Its edit names that value, which it did show, and keeps the other
/// person's text.
#[test]
fn an_edit_from_a_value_that_came_back_keeps_the_other_writer() {
    for (start, typed, back, then, edit, merged) in [
        (
            "Hello",
            "Hello!",
            "Hello",
            "Hello there, friend",
            "Why Hello",
            "Why Hello there, friend",
        ),
        ("", "hi", "", "Bob", "Z", "BobZ"),
    ] {
        let server = block_on(Db::open(config(0x64))).unwrap();
        let mut da = connect(block_on(Db::open(config(0x95))).unwrap(), &server);
        let mut db = connect(block_on(Db::open(config(0x96))).unwrap(), &server);
        insert(&da.db, start);
        settle(&server, &mut [&mut da, &mut db]);
        let mut alice = Editor::new(start, start.len());
        for c in typed[start.len()..].chars() {
            alice.type_char(&da.db, c);
        }
        settle(&server, &mut [&mut da, &mut db]);
        write_whole(&db.db, back);
        settle(&server, &mut [&mut da, &mut db]);
        alice.refresh(body(&da.db));
        assert_eq!(alice.text, back);
        write_whole(&db.db, then);
        settle(&server, &mut [&mut da, &mut db]);
        // Alice's store holds Bob's text; her editor still shows `back`.
        alice.cursor = 0;
        for c in edit[..edit.len() - back.len()].chars() {
            alice.type_char(&da.db, c);
        }
        settle(&server, &mut [&mut da, &mut db]);
        assert_eq!(body(&server), merged, "from {start:?}");
    }
}

/// Two editors over one column through one store each name the value they
/// showed, and both keep their text.
#[test]
fn two_editors_through_one_store_keep_both_edits() {
    let db = block_on(Db::open(config(0x65))).unwrap();
    insert(&db, "abc");
    let (mut one, mut two) = (Editor::new("abc", 3), Editor::new("abc", 3));
    one.type_char(&db, 'X');
    two.type_char(&db, 'Y');
    assert_eq!(body(&db), "abcXY");
}

/// An edit from the value the store holds is written as it is, even when
/// that value equals one of the store's own earlier writes (someone undid
/// its last edit, and the editor shows that).
#[test]
fn an_edit_from_the_held_value_is_written_as_is() {
    let db = block_on(Db::open(config(0x63))).unwrap();
    insert(&db, "Bring the ");
    let mut editor = Editor::new("Bring the ", 10);
    for c in "van".chars() {
        editor.type_char(&db, c);
    }
    write_whole(&db, "Bring the ");
    editor.refresh(body(&db));
    editor.cursor = editor.text.len();
    for c in "keys".chars() {
        editor.type_char(&db, c);
    }
    assert_eq!(body(&db), "Bring the keys");
}

/// Two or three people typing at the same spot, with keystrokes, sync
/// steps and renders in a random order (as in a browser: several
/// keystrokes before a round trip, renders whenever a value arrives):
/// every character typed is kept, in the order each person typed it, and
/// everyone ends with the same text.
#[test]
fn same_spot_typing_in_any_order_keeps_every_character() {
    // Letters, letters with spaces, and digits (a number anchors no diff).
    let typed = ["abcdefghijklmnopqrst", "ABCD EFGH IJKL MNOP", "0123456789"];
    // Short (inline), and over the 4 KiB threshold (chunked).
    let long = format!("{}\n\n", text(6 * 1024, 31));
    for (intro, people, seeds) in [("", 2usize, 30u64), ("", 3, 30), (long.as_str(), 2, 8)] {
        let start = format!("{intro}-- --");
        let at = intro.len() + 3;
        for seed in 1..=seeds {
            let server = block_on(Db::open(config(0x70))).unwrap();
            let mut devices: Vec<Device> = (0..people)
                .map(|n| connect(block_on(Db::open(config(0x97 + n as u8))).unwrap(), &server))
                .collect();
            insert(&devices[0].db, &start);
            settle(&server, &mut devices.iter_mut().collect::<Vec<_>>());
            let mut editors: Vec<Editor> = (0..people).map(|_| Editor::new(&start, at)).collect();
            let mut next = vec![0usize; people];
            let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let mut roll = |n: u64| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state % n
            };
            while (0..people).any(|who| next[who] < typed[who].chars().count()) {
                let who = roll(people as u64) as usize;
                let device = &devices[who];
                match roll(4) {
                    0 | 1 if next[who] < typed[who].chars().count() => {
                        let c = typed[who].chars().nth(next[who]).unwrap();
                        editors[who].type_char(&device.db, c);
                        next[who] += 1;
                    }
                    2 => block_on(device.db.tick()).unwrap(),
                    _ => {
                        block_on(server.tick()).unwrap();
                        editors[who].refresh(body(&device.db));
                    }
                }
            }
            settle(&server, &mut devices.iter_mut().collect::<Vec<_>>());
            let merged = body(&server);
            for (editor, device) in editors.iter_mut().zip(&devices) {
                editor.refresh(body(&device.db));
                assert_eq!(
                    editor.text,
                    merged,
                    "{people} people, {} bytes, seed {seed}",
                    start.len()
                );
            }
            for word in &typed[..people] {
                let mine: String = merged[intro.len()..]
                    .chars()
                    .filter(|c| *c != ' ' && word.contains(*c))
                    .collect();
                assert_eq!(
                    mine,
                    word.replace(' ', ""),
                    "{people} people, {} bytes, seed {seed}: {merged:?}",
                    start.len()
                );
            }
        }
    }
}

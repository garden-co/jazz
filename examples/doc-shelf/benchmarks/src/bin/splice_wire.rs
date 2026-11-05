//! EXPERIMENT (not for merge): what a keystroke costs on the wire and in
//! history, written whole (markdownSync today) or as a splice.
//!
//! A writer device and an observer device, both subscribed to the doc,
//! sync with an authority over counted in-memory links; all three are file
//! stores. `body` is a markdown-merge column. Run once per threshold with
//! `JAZZ_INLINE_VALUE_MAX_BYTES`.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, LargeValueUpdateSplice, ReadOpts,
    ReadTier, SubscriptionStream, TransportError, UpdateOptions, WireTransport,
    WireTransportAdapter, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Schema, TableName,
    TableSchema,
};
use jazz_example_doc_shelf_benchmark::markdown;

const KEYS: usize = 200;

fn config(node: u8) -> DbConfig {
    let schema = Schema::from([(
        TableName::new("docs"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("body", ColumnType::Text)
                    .merge_strategy(ColumnMergeStrategy::Markdown),
            ]),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: Default::default(),
            branch_by: Vec::new(),
        },
    )]);
    DbConfig::new(
        JazzSchema::new(&schema.allow_all()).expect("schema"),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

#[derive(Default, Clone, Copy)]
struct Traffic {
    bytes: usize,
}

struct Counted {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    sent: Rc<RefCell<Traffic>>,
}

impl WireTransport for Counted {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.sent.borrow_mut().bytes += frame.len();
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

struct Link {
    up: Rc<RefCell<Traffic>>,
    down: Rc<RefCell<Traffic>>,
}

fn link(device: &Db, server: &Db) -> (Link, impl Sized, impl Sized) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let up_sent = Rc::new(RefCell::new(Traffic::default()));
    let down_sent = Rc::new(RefCell::new(Traffic::default()));
    let client = Counted {
        outbound: Rc::clone(&up),
        inbound: Rc::clone(&down),
        sent: Rc::clone(&up_sent),
    };
    let authority = Counted {
        outbound: down,
        inbound: up,
        sent: Rc::clone(&down_sent),
    };
    let upstream = block_on(device.connect_upstream(Box::new(WireTransportAdapter::current(client))));
    let sub = server
        .accept_subscriber(
            Box::new(WireTransportAdapter::current(authority)),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    (
        Link {
            up: up_sent,
            down: down_sent,
        },
        upstream,
        sub,
    )
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum()
}

fn row() -> RowUuid {
    RowUuid::from_bytes([0xd0; 16])
}

fn body_of(db: &Db) -> Option<String> {
    let q = db
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(row().0))))
        .unwrap();
    let rows = block_on(db.all(&q, ReadOpts::default())).ok()?;
    match rows.first()?.cell("body") {
        Some(Value::String(s)) => Some(s),
        _ => None,
    }
}

fn run(threshold: usize, size: usize, splice: bool, keys: usize) -> Vec<u64> {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let open = |i: usize, node: u8| {
        block_on(Db::open_at(
            config(node),
            &dirs[i].path().join("s.sqlite"),
            FileOptions::default(),
        ))
        .unwrap()
    };
    let server = open(0, 0x50);
    let writer = open(1, 0x81);
    let observer = open(2, 0x82);
    let (w, _wu, _ws) = link(&writer, &server);
    let (o, _ou, _os) = link(&observer, &server);
    let remote = ReadOpts {
        tier: ReadTier::Remote,
        ..ReadOpts::default()
    };
    let q = Query::from("docs").filter(eq(col("id"), lit(row().0)));
    let wq = writer.prepare_query(&q).unwrap();
    let oq = observer.prepare_query(&q).unwrap();
    let mut wstream: SubscriptionStream = block_on(writer.subscribe(&wq, remote.clone())).unwrap();
    let mut ostream: SubscriptionStream = block_on(observer.subscribe(&oq, remote)).unwrap();
    let total = || w.up.borrow().bytes + w.down.borrow().bytes + o.up.borrow().bytes + o.down.borrow().bytes;
    let settle = |streams: &mut [&mut SubscriptionStream]| {
        let mut quiet = 0;
        let mut rounds = 0;
        while quiet < 3 && rounds < 2000 {
            let before = total();
            block_on(writer.tick()).unwrap();
            block_on(observer.tick()).unwrap();
            block_on(server.tick()).unwrap();
            block_on(writer.tick()).unwrap();
            block_on(observer.tick()).unwrap();
            for s in streams.iter_mut() {
                while s.try_next_event().is_some() {}
            }
            quiet = if total() == before { quiet + 1 } else { 0 };
            rounds += 1;
        }
    };
    let mut text = markdown(size as u64, size);
    block_on(writer.insert(
        "docs",
        BTreeMap::from([("body".to_owned(), Value::String(text.clone()))]),
        InsertOptions {
            row_id: Some(row()),
            ..Default::default()
        },
    ))
    .unwrap();
    settle(&mut [&mut wstream, &mut ostream]);
    assert_eq!(body_of(&observer).as_deref(), Some(text.as_str()), "observer synced");
    let snap = |l: &Link| (l.up.borrow().bytes, l.down.borrow().bytes);
    let (w0, o0) = (snap(&w), snap(&o));
    let stores0: Vec<u64> = dirs.iter().map(|d| dir_bytes(d.path())).collect();
    let mut cursor = size / 2;
    for k in 0..keys {
        let ch = if k % 6 == 5 { ' ' } else { (b'a' + (k % 26) as u8) as char };
        if splice {
            block_on(writer.splice_value(
                "docs",
                row(),
                "body",
                LargeValueUpdateSplice {
                    at: cursor as u64,
                    delete: 0,
                    insert: vec![ch as u8],
                },
                UpdateOptions::default(),
            ))
            .unwrap();
        } else {
            text.insert(cursor, ch);
            block_on(writer.update(
                "docs",
                row(),
                BTreeMap::from([("body".to_owned(), Value::String(text.clone()))]),
                UpdateOptions::default(),
            ))
            .unwrap();
        }
        if splice {
            text.insert(cursor, ch);
        }
        cursor += 1;
        settle(&mut [&mut wstream, &mut ostream]);
    }
    assert_eq!(body_of(&observer).as_deref(), Some(text.as_str()), "observer caught up");
    assert_eq!(body_of(&server).as_deref(), Some(text.as_str()), "authority caught up");
    let (w1, o1) = (snap(&w), snap(&o));
    drop((wstream, ostream, _wu, _ws, _ou, _os));
    drop((server, writer, observer));
    let stores1: Vec<u64> = dirs.iter().map(|d| dir_bytes(d.path())).collect();
    let _ = stores0;
    if keys == 0 {
        return stores1;
    }
    let base = run(threshold, size, splice, 0);
    let per = |a: usize, b: usize| (b - a) as f64 / KEYS as f64;
    let grow = |i: usize| (stores1[i] as f64 - base[i] as f64) / KEYS as f64;
    println!(
        "{}",
        serde_json::json!({
            "threshold": threshold, "doc_bytes": size,
            "writes": if splice { "splice" } else { "whole" },
            "wire_writer_up_per_key": per(w0.0, w1.0),
            "wire_writer_down_per_key": per(w0.1, w1.1),
            "wire_observer_down_per_key": per(o0.1, o1.1),
            "history_authority_per_key": grow(0),
            "history_writer_per_key": grow(1),
            "history_observer_per_key": grow(2),
        })
    );
    stores1
}

/// Two writers splice the same chunked doc concurrently (neither sees the
/// other's edit before writing): both edits must survive the authority's
/// markdown merge.
fn concurrent(size: usize) {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let open = |i: usize, node: u8| {
        block_on(Db::open_at(config(node), &dirs[i].path().join("s.sqlite"), FileOptions::default()))
            .unwrap()
    };
    let server = open(0, 0x50);
    let a = open(1, 0x81);
    let b = open(2, 0x82);
    let (la, _au, _as) = link(&a, &server);
    let (lb, _bu, _bs) = link(&b, &server);
    let remote = ReadOpts { tier: ReadTier::Remote, ..ReadOpts::default() };
    let q = Query::from("docs").filter(eq(col("id"), lit(row().0)));
    let mut sa = block_on(a.subscribe(&a.prepare_query(&q).unwrap(), remote.clone())).unwrap();
    let mut sb = block_on(b.subscribe(&b.prepare_query(&q).unwrap(), remote)).unwrap();
    let total = || la.up.borrow().bytes + la.down.borrow().bytes + lb.up.borrow().bytes + lb.down.borrow().bytes;
    let mut settle = || {
        let mut quiet = 0;
        while quiet < 3 {
            let before = total();
            for db in [&a, &b, &server, &a, &b] {
                block_on(db.tick()).unwrap();
            }
            while sa.try_next_event().is_some() {}
            while sb.try_next_event().is_some() {}
            quiet = if total() == before { quiet + 1 } else { 0 };
        }
    };
    let text = markdown(size as u64, size);
    block_on(a.insert("docs", BTreeMap::from([("body".to_owned(), Value::String(text.clone()))]),
        InsertOptions { row_id: Some(row()), ..Default::default() })).unwrap();
    settle();
    let splice = |db: &Db, at: usize, s: &str| {
        block_on(db.splice_value("docs", row(), "body",
            LargeValueUpdateSplice { at: at as u64, delete: 0, insert: s.as_bytes().to_vec() },
            UpdateOptions::default())).unwrap();
    };
    if std::env::var_os("PAIR_TYPING").is_some() {
        drop(settle);
        let base = dir_bytes(dirs[0].path());
        let mut settle2 = || {
            let mut quiet = 0;
            while quiet < 3 {
                let before = total();
                for db in [&a, &b, &server, &a, &b] {
                    block_on(db.tick()).unwrap();
                }
                while sa.try_next_event().is_some() {}
                while sb.try_next_event().is_some() {}
                quiet = if total() == before { quiet + 1 } else { 0 };
            }
        };
        let up0 = la.up.borrow().bytes + lb.up.borrow().bytes;
        let down0 = la.down.borrow().bytes + lb.down.borrow().bytes;
        // Both type at once: each keystroke is written before the other's
        // previous one arrives, so every write merges on the authority.
        for k in 0..100 {
            splice(&a, 10 + k, "a");
            splice(&b, size + k + 10 - 10 + k, "b");
            settle2();
        }
        let up = la.up.borrow().bytes + lb.up.borrow().bytes - up0;
        let down = la.down.borrow().bytes + lb.down.borrow().bytes - down0;
        drop((sa, sb));
        let grown = dir_bytes(dirs[0].path()) as f64 - base as f64;
        println!("{}", serde_json::json!({
            "pair_typing_doc_bytes": size, "keys": 200,
            "up_per_key": up as f64 / 200.0, "down_per_key": down as f64 / 200.0,
            "authority_store_growth_per_key_live": grown / 200.0,
            "converged": body_of(&a) == body_of(&server) && body_of(&b) == body_of(&server),
        }));
        return;
    }
    // Each writes three keystrokes before anything syncs.
    for (i, ch) in ["X", "Y", "Z"].iter().enumerate() {
        splice(&a, 10 + i, ch);
    }
    for (i, ch) in ["p", "q", "r"].iter().enumerate() {
        splice(&b, size - 10 + i, ch);
    }
    settle();
    let mut expected = text.clone();
    expected.insert_str(size - 10, "pqr");
    expected.insert_str(10, "XYZ");
    let (ba, bb, bs) = (body_of(&a), body_of(&b), body_of(&server));
    println!("{}", serde_json::json!({
        "concurrent_doc_bytes": size,
        "authority_has_both": bs.as_deref() == Some(expected.as_str()),
        "writers_converged": ba == bs && bb == bs,
    }));
}

fn main() {
    let threshold = jazz::large_values::inline_value_max_bytes();
    if std::env::args().nth(1).as_deref() == Some("concurrent") {
        for size in [2 * 1024, 12 * 1024, 100 * 1024] {
            concurrent(size);
        }
        return;
    }
    for size in [2 * 1024, 12 * 1024, 48 * 1024, 100 * 1024] {
        run(threshold, size, false, KEYS);
        run(threshold, size, true, KEYS);
    }
}

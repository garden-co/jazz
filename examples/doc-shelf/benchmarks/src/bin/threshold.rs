//! EXPERIMENT (not for merge): what the inline threshold costs and buys.
//!
//! Run once per threshold, with `JAZZ_INLINE_VALUE_MAX_BYTES` set; prints
//! one JSON line per measurement.
//!
//! - `typing`: one document of a given size, typed into for `KEYS`
//!   keystrokes at a cursor that advances, written two ways: whole values
//!   (what markdownSync does) and byte splices (the edit API; inline cells
//!   take whole writes, as the API refuses splices on them). Store growth
//!   per keystroke, write latency, and a full read afterwards.
//! - `library`: a notes library with a realistic size spread; store size,
//!   insert time, a title page, a `contains` search (and how many of the
//!   notes holding the phrase it finds), and opening notes in full.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, LargeValueUpdateSplice, ReadOpts,
    UpdateOptions, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, contains, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_example_doc_shelf_benchmark::markdown;

const KEYS: usize = 400;
const NOTES: usize = 2000;
const NEEDLE: &str = "kumquat protocol";

fn config() -> DbConfig {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .index_only(["title"]),
            )
            .build()
            .allow_all(),
    )
    .expect("schema");
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x61; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn id(n: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0xd0;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum()
}

fn open(path: &Path) -> Db {
    block_on(Db::open_at(config(), path, FileOptions::default())).expect("open")
}

fn insert(db: &Db, n: usize, body: String) {
    let w = block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("title".to_owned(), Value::String(format!("Note {n:05}"))),
            ("body".to_owned(), Value::String(body)),
        ]),
        InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        },
    ))
    .expect("insert");
    block_on(w.wait(DurabilityTier::Local)).unwrap();
}

fn read_body(db: &Db, n: usize) -> usize {
    let q = db
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(id(n).0))))
        .unwrap();
    let rows = block_on(db.all(&q, ReadOpts::default())).unwrap();
    match rows[0].cell("body") {
        Some(Value::String(s)) => s.len(),
        other => panic!("{other:?}"),
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn pct(mut v: Vec<f64>, p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p) as usize]
}

fn typing(threshold: usize, size: usize, splice: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.sqlite");
    let mut text = markdown(size as u64, size);
    let before;
    let mut lat = Vec::new();
    {
        let db = open(&path);
        insert(&db, 0, text.clone());
        drop(db);
        before = dir_bytes(dir.path());
        let db = open(&path);
        let mut cursor = size / 2;
        for k in 0..KEYS {
            let ch = if k % 6 == 5 { ' ' } else { (b'a' + (k % 26) as u8) as char };
            text.insert(cursor, ch);
            let chunked = text.len() > threshold;
            let t = Instant::now();
            let w = if splice && chunked {
                block_on(db.splice_value(
                    "docs",
                    id(0),
                    "body",
                    LargeValueUpdateSplice {
                        at: cursor as u64,
                        delete: 0,
                        insert: vec![ch as u8],
                    },
                    UpdateOptions::default(),
                ))
            } else {
                block_on(db.update(
                    "docs",
                    id(0),
                    BTreeMap::from([("body".to_owned(), Value::String(text.clone()))]),
                    UpdateOptions::default(),
                ))
            }
            .expect("write");
            block_on(w.wait(DurabilityTier::Local)).unwrap();
            lat.push(ms(t.elapsed()));
            cursor += 1;
        }
        let t = Instant::now();
        assert_eq!(read_body(&db, 0), text.len());
        let read = ms(t.elapsed());
        drop(db);
        let after = dir_bytes(dir.path());
        println!(
            "{}",
            serde_json::json!({
                "workload": "typing", "threshold": threshold, "doc_bytes": size,
                "writes": if splice { "splice" } else { "whole" },
                "bytes_per_key": (after as f64 - before as f64) / KEYS as f64,
                "store_after": after,
                "write_ms_p50": pct(lat.clone(), 0.5), "write_ms_p95": pct(lat, 0.95),
                "read_ms": read,
            })
        );
    }
}

/// Note sizes: 55% under 2 KiB, 25% 2-8 KiB, 15% 8-32 KiB, 5% 32-128 KiB.
fn note_len(n: usize) -> usize {
    let r = (n.wrapping_mul(2654435761)) % 100;
    let f = ((n.wrapping_mul(40503)) % 1000) as f64 / 1000.0;
    let (lo, hi) = match r {
        0..=54 => (200.0, 2048.0),
        55..=79 => (2048.0, 8192.0),
        80..=94 => (8192.0, 32768.0),
        _ => (32768.0, 131072.0),
    };
    (lo * (hi / lo as f64).powf(f)) as usize
}

fn needle(n: usize) -> bool {
    (n.wrapping_mul(7919)) % 53 == 0
}

fn library(threshold: usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("l.sqlite");
    let mut logical = 0;
    let mut needles = Vec::new();
    let t = Instant::now();
    {
        let db = open(&path);
        for n in 0..NOTES {
            let len = note_len(n);
            let mut body = markdown(n as u64 + 7, len);
            if needle(n) {
                body.insert_str(len / 2, NEEDLE);
                needles.push(n);
            }
            logical += body.len();
            insert(&db, n, body);
        }
    }
    let insert_ms = ms(t.elapsed());
    let store = dir_bytes(dir.path());
    let db = open(&path);
    let list = db
        .prepare_query(
            &Query::from("docs")
                .order_by("title", OrderDirection::Asc)
                .limit(50)
                .select(["title"]),
        )
        .unwrap();
    let search = db
        .prepare_query(
            &Query::from("docs")
                .filter(contains(col("body"), lit(Value::String(NEEDLE.to_owned()))))
                .select(["title"]),
        )
        .unwrap();
    let time = |f: &dyn Fn() -> usize, runs: usize| {
        let mut v = Vec::new();
        let mut out = 0;
        for _ in 0..runs {
            let t = Instant::now();
            out = f();
            v.push(ms(t.elapsed()));
        }
        (pct(v, 0.5), out)
    };
    let (list_ms, _) = time(&|| block_on(db.all(&list, ReadOpts::default())).unwrap().len(), 20);
    let (search_ms, found) =
        time(&|| block_on(db.all(&search, ReadOpts::default())).unwrap().len(), 10);
    let (open_ms, _) = time(
        &|| (0..NOTES).step_by(10).map(|n| read_body(&db, n)).sum(),
        3,
    );
    let chunked = (0..NOTES).filter(|&n| {
        let extra = if needle(n) { NEEDLE.len() } else { 0 };
        note_len(n) + extra > threshold
    });
    println!(
        "{}",
        serde_json::json!({
            "workload": "library", "threshold": threshold, "notes": NOTES,
            "logical_bytes": logical, "store_bytes": store,
            "chunked_notes": chunked.count(),
            "insert_ms_total": insert_ms,
            "title_page_ms": list_ms,
            "search_ms": search_ms, "search_found": found, "search_holding": needles.len(),
            "open_200_notes_ms": open_ms,
        })
    );
}

fn main() {
    let threshold = jazz::large_values::inline_value_max_bytes();
    let only = std::env::args().nth(1);
    if only.as_deref() != Some("library") {
        for size in [2 * 1024, 6 * 1024, 12 * 1024, 24 * 1024, 48 * 1024, 100 * 1024] {
            typing(threshold, size, false);
            typing(threshold, size, true);
        }
    }
    if only.as_deref() != Some("typing") {
        library(threshold);
    }
}

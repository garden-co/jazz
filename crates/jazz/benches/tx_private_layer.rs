//! SPIKE: open transactions' private queryable view, savepoint overlay vs
//! private layer (`JAZZ_TX_PRIVATE_LAYER=1`). Run once per mode:
//!
//! ```text
//! cargo bench -p jazz --bench tx_private_layer
//! JAZZ_TX_PRIVATE_LAYER=1 cargo bench -p jazz --bench tx_private_layer
//! ```
//!
//! For n staged writes it reports the mean latency of one stage and of one
//! point read inside the transaction, the commit latency, and the rows
//! SQLite wrote (`sqlite3_total_changes`, rolled-back ones included) per
//! staged write while staging and at commit. `quiet` runs the
//! transaction alone; `interrupted` lands a committed write outside the
//! transaction before each of its operations, which undoes a held
//! savepoint overlay so its next operation replays the batch (the shape
//! of a Durable Object, which cannot hold a SQL transaction between calls,
//! and of #5094's replay per operation). `read-heavy` reads a large table
//! inside a transaction that staged a few writes.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[path = "../tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

fn open() -> Db {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("title", ColumnType::Text)
                    .column("n", ColumnType::Integer)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::for_test_bytes([2; 16]),
        },
    )))
    .unwrap()
}

fn row(i: u64) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&i.to_be_bytes());
    bytes[15] = 1;
    RowUuid::from_bytes(bytes)
}

fn cells(i: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(format!("item {i}"))),
        ("n".to_owned(), Value::I32(i as i32)),
    ])
}

fn seed(db: &Db, rows: u64) {
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    for i in 0..rows {
        block_on(db.tx_ref(tx).insert(
            "items",
            cells(i),
            InsertOptions {
                row_id: Some(row(i)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(db.commit_mergeable_handle(tx)).unwrap();
}

struct Sample {
    stage: Duration,
    read: Duration,
    commit: Duration,
    stage_rows: f64,
    commit_rows: f64,
}

const SEEDED: u64 = 1_000;
/// The row a committed write outside the transaction lands on.
const OUTSIDE: u64 = SEEDED - 1;

/// n staged writes, half updates of seeded rows and half inserts, each
/// followed by a point read of the row it wrote.
fn staged(n: u64, interrupted: bool) -> Sample {
    let db = open();
    seed(&db, SEEDED);
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    let (mut stage, mut read) = (Duration::ZERO, Duration::ZERO);
    let mut rows = 0;
    for i in 0..n {
        if interrupted {
            block_on(db.update("items", row(OUTSIDE), cells(i), Default::default())).unwrap();
        }
        let target = if i % 2 == 0 {
            row(i / 2)
        } else {
            row(10_000 + i)
        };
        let before = db.sqlite_total_changes_for_test();
        let start = Instant::now();
        if i % 2 == 0 {
            block_on(
                db.tx_ref(tx)
                    .update("items", target, cells(i), Default::default()),
            )
            .unwrap();
        } else {
            block_on(db.tx_ref(tx).insert(
                "items",
                cells(i),
                InsertOptions {
                    row_id: Some(target),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        stage += start.elapsed();
        rows += db.sqlite_total_changes_for_test() - before;
        if interrupted {
            block_on(db.update("items", row(OUTSIDE), cells(i + 1), Default::default())).unwrap();
        }
        let start = Instant::now();
        let got = block_on(db.tx_ref(tx).read("items", target)).unwrap();
        read += start.elapsed();
        assert_eq!(got.unwrap()["n"], Value::I32(i as i32));
    }
    if interrupted {
        block_on(db.update("items", row(OUTSIDE), cells(0), Default::default())).unwrap();
    }
    let before = db.sqlite_total_changes_for_test();
    let start = Instant::now();
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    let commit = start.elapsed();
    let commit_rows = db.sqlite_total_changes_for_test() - before;
    Sample {
        stage: stage / n as u32,
        read: read / n as u32,
        commit,
        stage_rows: rows as f64 / n as f64,
        commit_rows: commit_rows as f64 / n as f64,
    }
}

/// A transaction that staged `writes` writes reads `reads` rows by id and
/// runs `scans` filtered scans of a `rows`-row table.
fn read_heavy(rows: u64, writes: u64, reads: u64, scans: u64) -> (Duration, Duration) {
    let db = open();
    seed(&db, rows);
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    for i in 0..writes {
        block_on(
            db.tx_ref(tx)
                .update("items", row(i * 7), cells(i), Default::default()),
        )
        .unwrap();
    }
    let start = Instant::now();
    for i in 0..reads {
        let got = block_on(db.tx_ref(tx).read("items", row((i * 7919) % rows))).unwrap();
        assert!(got.is_some());
    }
    let point = start.elapsed() / reads as u32;
    let scan = db
        .prepare_query(&db.table("items").filter(eq(col("title"), lit("item 3"))))
        .unwrap();
    let start = Instant::now();
    for _ in 0..scans {
        block_on(db.tx_ref(tx).all_prepared(&scan)).unwrap();
    }
    let scanned = start.elapsed() / scans as u32;
    block_on(db.commit_serializable_handle(tx)).unwrap();
    (point, scanned)
}

fn us(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e6)
}

fn main() {
    let mode = if std::env::var("JAZZ_TX_PRIVATE_LAYER").is_ok_and(|v| v == "1") {
        "private"
    } else {
        "savepoint"
    };
    let max: u64 = std::env::var("MAX_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    println!(
        "| mode | shape | n | stage µs/op | read µs/op | commit µs | rows/write staging | rows/write commit |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    for interrupted in [false, true] {
        for n in [1, 10, 100, 1000].into_iter().filter(|n| *n <= max) {
            let s = staged(n, interrupted);
            println!(
                "| {mode} | {} | {n} | {} | {} | {} | {:.1} | {:.1} |",
                if interrupted { "interrupted" } else { "quiet" },
                us(s.stage),
                us(s.read),
                us(s.commit),
                s.stage_rows,
                s.commit_rows,
            );
        }
    }
    let rows = std::env::var("ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let (point, scan) = read_heavy(rows, 10, 1000, 10);
    println!(
        "\nread-heavy ({mode}): {rows} rows, 10 staged writes: point read {} µs, filtered scan {} µs",
        us(point),
        us(scan)
    );
}

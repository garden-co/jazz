//! Shared benchmark plumbing: schema, RocksDB-backed `Db` construction, the
//! by-id read, the synthetic EBS delay, and the metrics report.

use std::path::Path;
use std::time::{Duration, Instant};

use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid};
use jazz::query::{Query, col, eq, in_list, lit};
use jazz::schema::{JazzSchema, TableSchema};

use crate::dataset::{FIELDS, ID_COL, TABLE};

pub(crate) fn schema() -> JazzSchema {
    let mut columns = vec![ColumnSchema::new(ID_COL, ColumnType::String)];
    columns.extend(
        FIELDS
            .iter()
            .map(|n| ColumnSchema::new(*n, ColumnType::String)),
    );
    JazzSchema::new([TableSchema::new(TABLE, columns)])
}

pub(crate) fn open_rocks_db(schema: &JazzSchema, path: &Path) -> Db<RocksDbStorage> {
    open_rocks_db_as(schema, path, [1u8; 16], AuthorId::SYSTEM)
}

pub(crate) fn open_rocks_db_as(
    schema: &JazzSchema,
    path: &Path,
    node: [u8; 16],
    author: AuthorId,
) -> Db<RocksDbStorage> {
    let cfs: Vec<String> = schema.column_families();
    let refs: Vec<&str> = cfs.iter().map(String::as_str).collect();
    let storage = RocksDbStorage::open_with_durability(path, &refs, Durability::WalNoSync)
        .expect("open rocksdb storage");
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes(node),
            author,
        },
    )))
    .expect("open db")
}

/// Fetch the sampled ids with a single membership read, then probe the per-id
/// point-lookup cost on a small sub-sample. Returns
/// `(bulk_read_time, rows_found, avg_per_id_lookup)`.
pub(crate) fn jazz_read_by_id(
    db: &Db<RocksDbStorage>,
    ids: &[String],
) -> (Duration, usize, Duration) {
    let query = Query::from(TABLE).filter(in_list(
        col(ID_COL),
        ids.iter().map(|id| lit(Value::String(id.clone()))),
    ));
    let prepared = db.prepare_query(&query).expect("prepare in_list query");
    let t = Instant::now();
    let rows = db.read(&prepared).expect("read in_list query");
    let bulk = t.elapsed();

    // Per-id point-lookup probe: Jazz's local read full-scans per query, so this
    // is the true "by id" cost. Averaged over a small sub-sample to stay fast.
    let probe_n = ids.len().min(16);
    let t = Instant::now();
    for id in ids.iter().take(probe_n) {
        let q = Query::from(TABLE).filter(eq(col(ID_COL), lit(Value::String(id.clone()))));
        let prepared = db.prepare_query(&q).expect("prepare point query");
        let _ = db.read(&prepared).expect("read point query");
    }
    let per_lookup = t.elapsed() / probe_n.max(1) as u32;
    (bulk, rows.len(), per_lookup)
}

/// Fixed latency charged once per durable commit batch, modelling a
/// network-attached volume. Applied identically to every topology's write loop.
#[derive(Clone, Copy)]
pub(crate) struct EbsDelay {
    per_batch: Duration,
}

impl EbsDelay {
    pub(crate) fn new(ms: u64) -> Self {
        Self {
            per_batch: Duration::from_millis(ms),
        }
    }
    pub(crate) fn charge(self) {
        if !self.per_batch.is_zero() {
            std::thread::sleep(self.per_batch);
        }
    }
}

pub(crate) struct Metrics {
    pub(crate) label: String,
    pub(crate) rows: usize,
    pub(crate) found: usize,
    pub(crate) read_kind: &'static str,
    pub(crate) write: Duration,
    pub(crate) flush: Duration,
    pub(crate) read_500: Duration,
    pub(crate) per_lookup: Option<Duration>,
    /// For the server topology: rows the server durably persisted (should equal
    /// `rows`). `None` for the local-only topologies.
    pub(crate) synced_to_server: Option<usize>,
}

impl Metrics {
    pub(crate) fn print(&self) {
        let rows_per_s = self.rows as f64 / self.write.as_secs_f64().max(1e-9);
        println!("═══ {} ═══", self.label);
        println!(
            "  write all           {:>8.3} s   ({:.0} rows/s)",
            self.write.as_secs_f64(),
            rows_per_s
        );
        println!("  flush / settle      {:>8.3} s", self.flush.as_secs_f64());
        if let Some(synced) = self.synced_to_server {
            println!("  synced to server    {:>8} / {} rows", synced, self.rows);
        }
        println!(
            "  get 500 by id       {:>8.3} ms  ({}, {}/{} found)",
            self.read_500.as_secs_f64() * 1e3,
            self.read_kind,
            self.found,
            500.min(self.rows),
        );
        if let Some(per) = self.per_lookup {
            let extrapolated = per.as_secs_f64() * 500.0;
            println!(
                "    per-id point lookup {:>6.3} ms  (500 sequential ≈ {:.1} s)",
                per.as_secs_f64() * 1e3,
                extrapolated
            );
        }
    }
}

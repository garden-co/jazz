//! One show's task list, synced between a file-backed worker (the durable
//! owner) and an in-memory foreground over an in-process link. Timed cases
//! cover the whole round trip through the public `Db` API; the profile binary
//! prints per-phase timings. This excludes JS, IndexedDB, scheduling and auth
//! bootstrap.
use std::{collections::BTreeMap, time::Instant};
mod perf_control;
mod support;
use jazz::{
    block_on,
    db::{
        CurrentRow, Db, DbConfig, DbIdentity, FileOptions, InsertOptions, PreparedQuery, ReadOpts,
        ReadTier, SubscriptionStream, UpdateOptions, Upstream, duplex,
    },
    ids::{AuthorSubject, NodeUuid, RowUuid},
    query::{Query, col, eq, lit},
    records::Value,
    schema::JazzSchema,
    tools::test_support::allow_all_policies,
    tools::{ColumnType, SchemaBuilder, TableSchemaBuilder},
};
use serde_json::json;
thread_local! { static REPORT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(unix)]
fn clock_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) },
        0
    );
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}
#[cfg(not(unix))]
fn clock_ns() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}
#[inline(never)]
fn phase<T>(backend: &str, count: usize, name: &str, f: impl FnOnce() -> T) -> T {
    if !REPORT.get() {
        return f();
    }
    let profile = perf_control::PerfControl::selected(backend, name);
    let cpu_profiled = profile.is_some();
    let start_ns = clock_ns();
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed();
    let end_ns = clock_ns();
    drop(profile);
    println!(
        "{}",
        json!({"rust_allocator":jazz_benchmark_guard::ALLOCATOR_NAME,"allocator_preload":std::env::var_os("LD_PRELOAD").is_some(),"backend":backend,"rows":count,"phase":name,"wall_us":elapsed.as_micros(),"start_ns":start_ns,"end_ns":end_ns,"cpu_profiled":cpu_profiled})
    );
    result
}
fn schema() -> JazzSchema {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("tasks")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .policies(allow_all_policies()),
        )
        .build();
    JazzSchema::new(&schema).unwrap()
}
fn row(i: usize) -> RowUuid {
    RowUuid::from_bytes((i as u128 + 1).to_be_bytes())
}
fn cells(i: usize, done: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".into(), Value::String(format!("Task {i}"))),
        ("done".into(), Value::Bool(done)),
    ])
}
fn identity(id: u8) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([id; 16]),
        author: AuthorSubject::SYSTEM,
    }
}
fn open_worker(schema: &JazzSchema, dir: &tempfile::TempDir) -> Db {
    block_on(Db::open_at(
        DbConfig::new(schema.clone(), identity(1)),
        &dir.path().join("tasks.sqlite"),
        FileOptions::default(),
    ))
    .unwrap()
}
fn local() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    }
}
/// Run both ends until the link is quiet: the foreground uploads, the worker
/// applies and answers, the foreground applies the answer.
fn pump(worker: &Db, foreground: &Db) {
    for _ in 0..4 {
        block_on(foreground.tick()).unwrap();
        block_on(worker.tick()).unwrap();
    }
    block_on(foreground.tick()).unwrap();
}

/// The foreground's side of a loaded task list.
struct Foreground {
    db: Db,
    _upstream: Upstream,
    _stream: SubscriptionStream,
}

/// Prepared worker/foreground workload. Every write is authored on the
/// foreground and counted done once the worker has it and the foreground's
/// subscription has settled.
pub struct Fixture {
    backend: &'static str,
    count: usize,
    schema: JazzSchema,
    dir: tempfile::TempDir,
    worker: Option<Db>,
    foreground: Option<Foreground>,
    all: Option<PreparedQuery>,
    changed: usize,
    read_result: Vec<CurrentRow>,
}
impl Fixture {
    /// A worker database seeded with `count` unfinished tasks, closed.
    pub fn seeded(count: usize) -> Self {
        if !REPORT.get() {
            jazz_benchmark_guard::refuse_contaminated_measurement();
        }
        assert!((2..=4096).contains(&count));
        let backend = "sqlite_wal";
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();
        let worker = open_worker(&schema, &dir);
        phase(backend, count, "seed", || {
            let tx = block_on(worker.mergeable_tx()).unwrap();
            for i in 0..count {
                block_on(tx.insert(
                    "tasks",
                    cells(i, false),
                    InsertOptions {
                        row_id: Some(row(i)),
                        ..Default::default()
                    },
                ))
                .unwrap();
            }
            block_on(tx.commit()).unwrap();
        });
        block_on(worker.close()).unwrap();
        Self {
            backend,
            count,
            schema,
            dir,
            worker: None,
            foreground: None,
            all: None,
            changed: 0,
            read_result: Vec::new(),
        }
    }
    pub fn loaded(count: usize) -> Self {
        let mut fixture = Self::seeded(count);
        fixture.reopen();
        fixture
    }
    /// Reopen the worker from its file and load a fresh foreground: connect,
    /// subscribe to the whole list and sync it.
    #[inline(never)]
    pub fn reopen(&mut self) {
        self.foreground = None;
        if let Some(worker) = self.worker.take() {
            block_on(worker.close()).unwrap();
        }
        let worker = phase(self.backend, self.count, "reopen", || {
            open_worker(&self.schema, &self.dir)
        });
        let foreground = phase(self.backend, self.count, "load", || {
            let db = block_on(Db::open(DbConfig::new(self.schema.clone(), identity(2)))).unwrap();
            let (client_side, server_side) = duplex();
            let upstream = block_on(db.connect_upstream(client_side));
            worker.accept_subscriber(server_side, AuthorSubject::SYSTEM);
            let all = db.prepare_query(&Query::from("tasks")).unwrap();
            let stream = block_on(db.subscribe(&all, ReadOpts::default())).unwrap();
            pump(&worker, &db);
            self.all = Some(all);
            Foreground {
                db,
                _upstream: upstream,
                _stream: stream,
            }
        });
        self.worker = Some(worker);
        self.foreground = Some(foreground);
        self.read_all();
    }
    #[inline(never)]
    pub fn batch_update(&mut self, changed: usize) {
        assert!(self.changed == 0 && changed <= self.count);
        self.write_range(0..changed, false);
        self.changed = changed;
        self.read_all();
    }
    #[inline(never)]
    pub fn sequential_update(&mut self, changed: usize) {
        assert!(self.changed == 0 && changed <= self.count);
        for i in 0..changed {
            self.write_range(i..i + 1, false);
        }
        self.changed = changed;
        self.read_all();
    }
    /// Grow an already subscribed table with one new row per roundtrip.
    /// Caller-supplied IDs and cells are constructed inside the timed operation.
    #[inline(never)]
    pub fn sequential_insert(&mut self, inserted: usize) {
        assert_eq!(
            self.changed, 0,
            "insert workload starts with unfinished tasks"
        );
        let start = self.count;
        for i in start..start + inserted {
            self.count += 1;
            self.write_range(i..i + 1, true);
        }
        self.read_all();
    }
    fn read_all(&mut self) {
        let foreground = self.foreground.as_ref().expect("loaded");
        self.read_result = block_on(
            foreground
                .db
                .all(self.all.as_ref().expect("loaded"), local()),
        )
        .unwrap();
    }
    pub fn verify(&mut self) {
        assert_eq!(self.read_result.len(), self.count);
        let actual = self
            .read_result
            .iter()
            .map(|value| (value.row_uuid(), value))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual.len(), self.count);
        for i in 0..self.count {
            let value = actual
                .get(&row(i))
                .expect("expected task present in timed read");
            assert_eq!(
                value.cell("title"),
                Some(Value::String(format!("Task {i}")))
            );
            assert_eq!(value.cell("done"), Some(Value::Bool(i < self.changed)));
        }
        if REPORT.get() {
            // The worker holds exactly the done tasks the foreground shows.
            let worker = self.worker.as_ref().expect("loaded");
            let done = worker
                .prepare_query(&Query::from("tasks").filter(eq(col("done"), lit(true))))
                .unwrap();
            let rows = phase(self.backend, self.count, "worker_done_query", || {
                worker.read(&done).unwrap()
            });
            assert_eq!(
                rows.iter()
                    .map(|r| r.row_uuid())
                    .collect::<std::collections::BTreeSet<_>>(),
                (0..self.changed).map(row).collect()
            );
        }
    }
    fn write_range(&mut self, range: std::ops::Range<usize>, insert: bool) {
        let (backend, count) = (self.backend, self.count);
        let worker = self.worker.as_ref().expect("reopen before updates");
        let foreground = &self.foreground.as_ref().expect("reopen before updates").db;
        phase(backend, count, "author", || {
            let tx = block_on(foreground.mergeable_tx()).unwrap();
            for i in range.clone() {
                if insert {
                    block_on(tx.insert(
                        "tasks",
                        cells(i, false),
                        InsertOptions {
                            row_id: Some(row(i)),
                            ..Default::default()
                        },
                    ))
                    .unwrap();
                } else {
                    block_on(tx.update("tasks", row(i), cells(i, true), UpdateOptions::default()))
                        .unwrap();
                }
            }
            block_on(tx.commit()).unwrap();
        });
        phase(backend, count, "sync", || pump(worker, foreground));
    }
}
fn run(count: usize, percent: usize) {
    let mut fixture = Fixture::seeded(count);
    fixture.reopen();
    fixture.batch_update(count * percent / 100);
    fixture.reopen();
    fixture.verify();
}

/// Batch update and reopen over a small list; assertions check exact rows.
pub fn correctness_smoke() {
    run(10, 50);
}

pub fn profile_main() {
    REPORT.set(true);
    if std::env::var_os("JAZZ_PERF_PHASE").is_some() {
        eprintln!("scoped CPU attribution run: timings are not clean latency receipts");
    } else {
        jazz_benchmark_guard::refuse_contaminated_measurement();
    }
    let percent = support::env_usize("JAZZ_BATCH_UPDATE_PERCENT", 90);
    let workload = std::env::var("JAZZ_TODO_WORKLOAD").unwrap_or_else(|_| "batch".into());
    assert!(matches!(
        workload.as_str(),
        "batch" | "sequential-insert" | "sequential-update"
    ));
    for count in support::csv_usizes("JAZZ_BATCH_ROWS", "1500") {
        if workload == "batch" {
            run(count, percent);
        } else {
            assert!((20..=4096).contains(&count));
            let initial = if workload == "sequential-insert" {
                count / 10
            } else {
                count
            };
            let mut fixture = Fixture::loaded(initial);
            if workload == "sequential-insert" {
                fixture.sequential_insert(count - initial);
            } else {
                fixture.sequential_update(count * percent / 100);
            }
            fixture.verify();
            fixture.reopen();
            fixture.verify();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sequential_inserts_deliver_and_persist_exact_rows() {
        let mut fixture = Fixture::loaded(2);
        fixture.verify();
        fixture.sequential_insert(9);
        fixture.verify();
        fixture.sequential_insert(9);
        fixture.verify();
        fixture.reopen();
        fixture.verify();
    }
    #[test]
    fn batch_reopen_preserves_exact_rows() {
        correctness_smoke();
    }
    #[test]
    fn updates_return_current_rows_before_reopen() {
        for sequential in [false, true] {
            let mut f = Fixture::loaded(10);
            f.verify();
            if sequential {
                f.sequential_update(9);
            } else {
                f.batch_update(9);
            }
            f.verify();
            f.reopen();
            f.verify();
        }
    }
}

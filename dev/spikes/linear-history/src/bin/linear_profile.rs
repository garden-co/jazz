//! Phase receipts for the linear-history spike, shaped like the todo-profile
//! local-batch workload (a `tasks` table with `title` and indexed `done`,
//! seeded in one transaction, then P% of rows updated in one transaction).
//!
//! Env: `LH_ROWS` (1500), `LH_UPDATE_PERCENT` (90), `LH_DEPTH` (10 extra
//! update rounds before read phases), `LH_REPEATS` (5; the minimum time is
//! reported). Prints one JSON object per phase, then a table.

use std::time::Instant;

use groove::storage::{BoxedStorage, MemoryStorage, OrderedKvStorage};
use jazz_storage_rocksdb::RocksDbStorage;
use linear_history::codec::{COLUMN_FAMILIES, encode_row};
use linear_history::merges::{counter_i64, i64_value};
use linear_history::storage::{CountHandle, Counting, OpCounts, block_on};
use linear_history::*;

const TASKS: TableId = 1;
const TITLE: usize = 0;
const DONE: usize = 1;
const COUNT: usize = 2;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn schema() -> Schema {
    Schema::new([TableDef::new(TASKS)
        .column("title")
        .indexed_column("done")
        .merged_column("count", counter_i64)])
}

#[derive(Clone, Debug)]
struct Phase {
    name: &'static str,
    ms: f64,
    ops: OpCounts,
    rows: usize,
    extra: String,
}

struct Run {
    core: Authority<Counting<BoxedStorage>>,
    counts: CountHandle,
    client: Client,
    phases: Vec<Phase>,
    _dir: Option<tempfile::TempDir>,
}

impl Run {
    fn new(backend: &str, options: AuthorityOptions) -> Self {
        let (storage, dir) = match backend {
            "memory" => (
                BoxedStorage::new(MemoryStorage::new(&COLUMN_FAMILIES).unwrap()),
                None,
            ),
            _ => {
                let dir = tempfile::tempdir().unwrap();
                let rocks = RocksDbStorage::open(dir.path(), &COLUMN_FAMILIES).unwrap();
                (BoxedStorage::new(rocks), Some(dir))
            }
        };
        let (storage, counts) = Counting::new(storage);
        Self {
            core: Authority::open(storage, schema(), options).unwrap(),
            counts,
            client: Client::new(1, schema()),
            phases: Vec::new(),
            _dir: dir,
        }
    }

    fn measure<T>(
        &mut self,
        name: &'static str,
        rows: usize,
        f: impl FnOnce(&mut Self) -> (T, String),
    ) -> T {
        let before = self.counts.snapshot();
        let start = Instant::now();
        let (value, extra) = f(self);
        let ms = start.elapsed().as_secs_f64() * 1e3;
        let ops = self.counts.snapshot().since(before);
        self.phases.push(Phase {
            name,
            ms,
            ops,
            rows,
            extra,
        });
        value
    }

    fn apply(&mut self, tx: &Tx) -> Seq {
        let outcome = self.core.apply(tx).unwrap();
        self.client.settle(tx.id);
        match outcome {
            Outcome::Accepted(seq) => seq,
            other => panic!("unexpected {other:?}"),
        }
    }

    fn sync(&mut self, since: Seq) {
        let rows = self.core.changes_since(TASKS, since).unwrap();
        self.client.receive(TASKS, rows);
    }
}

fn updated_rows(rows: usize, percent: usize, round: usize) -> Vec<u128> {
    let n = rows * percent / 100;
    (0..n).map(|i| ((i + round * 7) % rows) as u128).collect()
}

fn one_run(
    backend: &str,
    options: AuthorityOptions,
    rows: usize,
    percent: usize,
    depth: usize,
) -> Vec<Phase> {
    let mut run = Run::new(backend, options);
    let seed_cut = run.measure("seed_one_tx", rows, |r| {
        let tx = (0..rows as u128)
            .fold(r.client.begin(), |t, row| {
                t.set(
                    TASKS,
                    row,
                    TITLE,
                    Some(format!("task number {row:>8}").into_bytes()),
                )
                .set(TASKS, row, DONE, Some(b"false".to_vec()))
                .set(TASKS, row, COUNT, i64_value(0))
            })
            .commit();
        (r.apply(&tx), String::new())
    });
    run.sync(0);

    let update = updated_rows(rows, percent, 0);
    let after_update = run.measure("update_lww_one_tx", update.len(), |r| {
        let tx = update
            .iter()
            .fold(r.client.begin(), |t, &row| {
                t.set(TASKS, row, DONE, Some(b"true".to_vec()))
            })
            .commit();
        (r.apply(&tx), String::new())
    });
    run.sync(seed_cut);

    run.measure("update_threeway_counter_one_tx", update.len(), |r| {
        let tx = update
            .iter()
            .fold(r.client.begin(), |t, &row| {
                t.merge(TASKS, row, COUNT, i64_value(1))
            })
            .commit();
        (r.apply(&tx), String::new())
    });
    run.sync(after_update);

    let per_row: Vec<u128> = update.iter().copied().take(rows.min(1350)).collect();
    run.measure("update_lww_tx_per_row", per_row.len(), |r| {
        for &row in &per_row {
            let tx = r
                .client
                .begin()
                .set(TASKS, row, TITLE, Some(b"renamed".to_vec()))
                .commit();
            r.apply(&tx);
        }
        ((), String::new())
    });

    let mut cut = run.core.last_seq();
    run.measure("history_depth_rounds", depth * update.len(), |r| {
        for round in 1..=depth {
            let rows_this_round = updated_rows(rows, percent, round);
            let flag: &[u8] = if round % 2 == 0 { b"true" } else { b"false" };
            let tx = rows_this_round
                .iter()
                .fold(r.client.begin(), |t, &row| {
                    t.set(TASKS, row, DONE, Some(flag.to_vec()))
                })
                .commit();
            cut = r.apply(&tx);
        }
        ((), format!("rounds={depth}"))
    });
    // A small tail (1% of rows) after `cut`, so a recent-cut read has a
    // short change log to rewind.
    let tail: Vec<u128> = (0..(rows / 100).max(1) as u128).collect();
    run.measure("update_small_tail_one_tx", tail.len(), |r| {
        let tx = tail
            .iter()
            .fold(r.client.begin(), |t, &row| {
                t.set(TASKS, row, DONE, Some(b"tail".to_vec()))
            })
            .commit();
        (r.apply(&tx), String::new())
    });
    run.sync(0);
    block_on(run.core.storage().flush_write_boundary()).unwrap();
    let now = run.core.last_seq();

    run.measure("read_current_scan", rows, |r| {
        let n = r.core.scan_at(TASKS, now).unwrap().len();
        ((), format!("visible={n}"))
    });
    run.measure("read_point_current_all", rows, |r| {
        for row in 0..rows as u128 {
            r.core.get(TASKS, row).unwrap();
        }
        ((), String::new())
    });
    run.measure("read_point_at_seed_cut_all", rows, |r| {
        for row in 0..rows as u128 {
            r.core.get_at(TASKS, row, seed_cut).unwrap();
        }
        ((), String::new())
    });
    run.measure("read_scan_at_seed_cut", rows, |r| {
        let n = r.core.scan_at(TASKS, seed_cut).unwrap().len();
        ((), format!("visible={n}"))
    });
    for (name, at) in [
        ("query_eq_at_seed_cut", seed_cut),
        ("query_eq_at_recent_cut", cut),
    ] {
        for strategy in [SnapshotStrategy::Forward, SnapshotStrategy::Rewind] {
            let label: &'static str = match (name, strategy) {
                ("query_eq_at_seed_cut", SnapshotStrategy::Forward) => {
                    "query_eq_at_seed_cut_forward"
                }
                ("query_eq_at_seed_cut", SnapshotStrategy::Rewind) => "query_eq_at_seed_cut_rewind",
                (_, SnapshotStrategy::Forward) => "query_eq_at_recent_cut_forward",
                (_, SnapshotStrategy::Rewind) => "query_eq_at_recent_cut_rewind",
            };
            run.measure(label, rows, |r| {
                let n = r
                    .core
                    .lookup_eq_at(TASKS, DONE, b"false", at, strategy)
                    .unwrap()
                    .len();
                ((), format!("matches={n}"))
            });
        }
    }
    run.measure("sync_changes_since_recent_cut", tail.len(), |r| {
        let changes = r.core.changes_since(TASKS, cut).unwrap();
        let bytes: usize = changes.iter().map(|(_, i)| 16 + encode_row(i).len()).sum();
        ((), format!("rows={} payload_bytes={bytes}", changes.len()))
    });
    run.measure("sync_changes_since_seed_cut", rows, |r| {
        let changes = r.core.changes_since(TASKS, seed_cut).unwrap();
        let bytes: usize = changes.iter().map(|(_, i)| 16 + encode_row(i).len()).sum();
        ((), format!("rows={} payload_bytes={bytes}", changes.len()))
    });
    run.measure("exclusive_predicate_validate_conflict", 1, |r| {
        let tx = r
            .client
            .begin()
            .set(TASKS, 1_000_000, TITLE, Some(b"x".to_vec()))
            .commit_as(TxKind::Exclusive {
                base: seed_cut,
                rows_read: vec![],
                predicates: vec![EqPredicate {
                    table: TASKS,
                    column: DONE,
                    value: Some(b"false".to_vec()),
                }],
            });
        let outcome = r.core.apply(&tx).unwrap();
        ((), format!("{outcome:?}"))
    });
    run.measure("exclusive_predicate_validate_clean", 1, |r| {
        let tx = r
            .client
            .begin()
            .set(TASKS, 1_000_001, TITLE, Some(b"x".to_vec()))
            .commit_as(TxKind::Exclusive {
                base: r.core.last_seq(),
                rows_read: vec![(TASKS, 1)],
                predicates: vec![EqPredicate {
                    table: TASKS,
                    column: DONE,
                    value: Some(b"false".to_vec()),
                }],
            });
        let outcome = r.core.apply(&tx).unwrap();
        ((), format!("{outcome:?}"))
    });
    run.phases
}

fn main() {
    let rows = env("LH_ROWS", 1500);
    let percent = env("LH_UPDATE_PERCENT", 90);
    let depth = env("LH_DEPTH", 10);
    let repeats = env("LH_REPEATS", 5).max(1);
    println!(
        "# linear-history spike: rows={rows} update_percent={percent} depth={depth} repeats={repeats} (min ms)"
    );
    for backend in ["memory", "rocksdb"] {
        for separate_current in [true, false] {
            let options = AuthorityOptions { separate_current };
            let mut best: Vec<Phase> = one_run(backend, options, rows, percent, depth);
            for _ in 1..repeats {
                for (b, p) in best
                    .iter_mut()
                    .zip(one_run(backend, options, rows, percent, depth))
                {
                    if p.ms < b.ms {
                        b.ms = p.ms;
                    }
                }
            }
            let layout = if separate_current {
                "current+history"
            } else {
                "history-only"
            };
            println!("\n## backend={backend} layout={layout}");
            println!(
                "{:<40} {:>9} {:>7} {:>8} {:>8} {:>10} {:>8} {:>7} {:>9}  extra",
                "phase", "ms", "rows", "writes", "w/row", "w_bytes", "gets", "scans", "scanned"
            );
            for p in &best {
                let per_row = if p.rows > 0 {
                    p.ops.writes as f64 / p.rows as f64
                } else {
                    0.0
                };
                println!(
                    "{:<40} {:>9.3} {:>7} {:>8} {:>8.2} {:>10} {:>8} {:>7} {:>9}  {}",
                    p.name,
                    p.ms,
                    p.rows,
                    p.ops.writes,
                    per_row,
                    p.ops.written_bytes,
                    p.ops.point_reads,
                    p.ops.scans,
                    p.ops.scanned_items,
                    p.extra
                );
                eprintln!(
                    "{{\"backend\":\"{backend}\",\"layout\":\"{layout}\",\"phase\":\"{}\",\"ms\":{:.4},\"rows\":{},\"writes\":{},\"written_bytes\":{},\"point_reads\":{},\"scans\":{},\"scanned_items\":{},\"extra\":\"{}\"}}",
                    p.name,
                    p.ms,
                    p.rows,
                    p.ops.writes,
                    p.ops.written_bytes,
                    p.ops.point_reads,
                    p.ops.scans,
                    p.ops.scanned_items,
                    p.extra
                );
            }
        }
    }
}

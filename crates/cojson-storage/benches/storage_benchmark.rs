//! Benchmarks comparing BTreeStorage vs SQLite performance.
//!
//! These benchmarks measure the performance of key storage operations
//! to validate the 2x+ performance improvement target over SQLite.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use rusqlite::{params, Connection, Result as SqliteResult};
use std::collections::HashMap;
use tempfile::tempdir;

use cojson_storage::bftree::{BTreeConfig, BTreeStorage};
use cojson_storage::types::*;
use cojson_storage::{StorageBackend, StorageTransaction};

// ============================================================================
// SQLite Storage Implementation for Benchmarking
// ============================================================================

/// A minimal SQLite storage implementation for benchmark comparison.
/// This mirrors the schema from the TypeScript SQLite storage.
struct SqliteStorage {
    conn: Connection,
}

impl SqliteStorage {
    fn new(path: &str) -> SqliteResult<Self> {
        let conn = Connection::open(path)?;

        // Create schema matching TypeScript storage
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;

            CREATE TABLE IF NOT EXISTS covalues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                covalue_id TEXT NOT NULL UNIQUE,
                header TEXT
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                covalue INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                last_idx INTEGER NOT NULL,
                last_signature TEXT NOT NULL,
                bytes_since_last_signature INTEGER,
                UNIQUE(covalue, session_id)
            );

            CREATE TABLE IF NOT EXISTS transactions (
                ses INTEGER NOT NULL,
                idx INTEGER NOT NULL,
                tx TEXT NOT NULL,
                PRIMARY KEY(ses, idx)
            );

            CREATE TABLE IF NOT EXISTS signatures (
                ses INTEGER NOT NULL,
                idx INTEGER NOT NULL,
                signature TEXT NOT NULL,
                PRIMARY KEY(ses, idx)
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_covalue ON sessions(covalue);
            CREATE INDEX IF NOT EXISTS idx_transactions_ses ON transactions(ses);
            CREATE INDEX IF NOT EXISTS idx_signatures_ses ON signatures(ses);
            "#,
        )?;

        Ok(Self { conn })
    }

    fn in_memory() -> SqliteResult<Self> {
        Self::new(":memory:")
    }

    fn upsert_covalue(&self, covalue_id: &str, header: Option<&str>) -> SqliteResult<i64> {
        // Try to get existing
        let existing: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM covalues WHERE covalue_id = ?",
                params![covalue_id],
                |row| row.get(0),
            )
            .ok();

        if let Some(id) = existing {
            Ok(id)
        } else if let Some(h) = header {
            self.conn.execute(
                "INSERT INTO covalues (covalue_id, header) VALUES (?, ?)",
                params![covalue_id, h],
            )?;
            Ok(self.conn.last_insert_rowid())
        } else {
            Ok(-1)
        }
    }

    fn get_covalue(&self, covalue_id: &str) -> SqliteResult<Option<(i64, String)>> {
        self.conn
            .query_row(
                "SELECT id, covalue_id FROM covalues WHERE covalue_id = ?",
                params![covalue_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
    }

    fn get_sessions(&self, covalue_id: i64) -> SqliteResult<Vec<(i64, String, i64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, session_id, last_idx FROM sessions WHERE covalue = ?")?;
        let rows = stmt.query_map(params![covalue_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
        rows.collect()
    }

    fn get_transactions(&self, session_id: i64, from_idx: i64, to_idx: i64) -> SqliteResult<Vec<(i64, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT idx, tx FROM transactions WHERE ses = ? AND idx >= ? AND idx < ?")?;
        let rows = stmt.query_map(params![session_id, from_idx, to_idx], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        rows.collect()
    }

    fn add_session(
        &self,
        covalue_id: i64,
        session_id: &str,
        last_idx: i64,
        signature: &str,
    ) -> SqliteResult<i64> {
        self.conn.execute(
            "INSERT OR REPLACE INTO sessions (covalue, session_id, last_idx, last_signature) VALUES (?, ?, ?, ?)",
            params![covalue_id, session_id, last_idx, signature],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn add_transaction(&self, session_id: i64, idx: i64, tx: &str) -> SqliteResult<()> {
        self.conn.execute(
            "INSERT INTO transactions (ses, idx, tx) VALUES (?, ?, ?)",
            params![session_id, idx, tx],
        )?;
        Ok(())
    }
}

// ============================================================================
// Test Data Generators
// ============================================================================

fn generate_covalue_id(idx: usize) -> String {
    format!("co_benchmark_{:08}", idx)
}

fn generate_session_id(idx: usize) -> String {
    format!("session_{:04}", idx)
}

fn generate_transaction_json(idx: usize) -> String {
    format!(
        r#"{{"op":"set","path":"/key{}","value":"value{}","timestamp":{}}}"#,
        idx,
        idx,
        1700000000 + idx
    )
}

fn generate_header() -> CoValueHeader {
    CoValueHeader {
        covalue_type: "comap".to_string(),
        ..Default::default()
    }
}

// ============================================================================
// Benchmark Functions
// ============================================================================

/// Benchmark single CoValue writes
fn bench_single_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_write");

    // BTreeStorage
    group.bench_function("btree", |b| {
        let storage = BTreeStorage::new(BTreeConfig::default());
        let header = generate_header();
        let mut idx = 0usize;

        b.iter(|| {
            let id = generate_covalue_id(idx);
            let _ = storage.upsert_covalue(black_box(&id), black_box(Some(&header)));
            idx += 1;
        });
    });

    // SQLite
    group.bench_function("sqlite", |b| {
        let storage = SqliteStorage::in_memory().unwrap();
        let header_json = r#"{"type":"comap"}"#;
        let mut idx = 0usize;

        b.iter(|| {
            let id = generate_covalue_id(idx);
            let _ = storage.upsert_covalue(black_box(&id), black_box(Some(header_json)));
            idx += 1;
        });
    });

    group.finish();
}

/// Benchmark bulk CoValue writes
fn bench_bulk_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_write");
    let sizes = [100, 1000, 10000];

    for size in sizes {
        group.throughput(Throughput::Elements(size as u64));

        // BTreeStorage
        group.bench_with_input(BenchmarkId::new("btree", size), &size, |b, &size| {
            b.iter_with_setup(
                || BTreeStorage::new(BTreeConfig::default()),
                |storage| {
                    let header = generate_header();
                    for i in 0..size {
                        let id = generate_covalue_id(i);
                        storage.upsert_covalue(&id, Some(&header));
                    }
                },
            );
        });

        // SQLite
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            b.iter_with_setup(
                || SqliteStorage::in_memory().unwrap(),
                |storage| {
                    let header_json = r#"{"type":"comap"}"#;
                    for i in 0..size {
                        let id = generate_covalue_id(i);
                        let _ = storage.upsert_covalue(&id, Some(header_json));
                    }
                },
            );
        });
    }

    group.finish();
}

/// Benchmark single CoValue reads
fn bench_single_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_read");
    let num_covalues = 10000;

    // Setup: Create storage with data
    let btree_storage = BTreeStorage::new(BTreeConfig::default());
    let sqlite_storage = SqliteStorage::in_memory().unwrap();

    let header = generate_header();
    let header_json = r#"{"type":"comap"}"#;

    for i in 0..num_covalues {
        let id = generate_covalue_id(i);
        btree_storage.upsert_covalue(&id, Some(&header));
        let _ = sqlite_storage.upsert_covalue(&id, Some(header_json));
    }

    // BTreeStorage
    group.bench_function("btree", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..num_covalues);
            let id = generate_covalue_id(idx);
            let _ = btree_storage.get_covalue(black_box(&id));
        });
    });

    // SQLite
    group.bench_function("sqlite", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..num_covalues);
            let id = generate_covalue_id(idx);
            let _ = sqlite_storage.get_covalue(black_box(&id));
        });
    });

    group.finish();
}

/// Benchmark session and transaction writes (simulating CRDT operations)
fn bench_transaction_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_write");
    let tx_counts = [10, 100, 1000];

    for tx_count in tx_counts {
        group.throughput(Throughput::Elements(tx_count as u64));

        // BTreeStorage
        group.bench_with_input(
            BenchmarkId::new("btree", tx_count),
            &tx_count,
            |b, &tx_count| {
                b.iter_with_setup(
                    || {
                        let storage = BTreeStorage::new(BTreeConfig::default());
                        let header = generate_header();
                        let row_id = storage.upsert_covalue("co_test", Some(&header)).unwrap();
                        (storage, row_id)
                    },
                    |(storage, covalue_row_id)| {
                        storage
                            .transaction(|tx| {
                                let session_update = SessionUpdate {
                                    session_update: SessionRow {
                                        covalue: covalue_row_id,
                                        session_id: "session_bench".to_string(),
                                        last_idx: tx_count as u64 - 1,
                                        last_signature: "sig_bench".to_string(),
                                        bytes_since_last_signature: None,
                                    },
                                    session_row: None,
                                };
                                let session_row_id = tx.add_session_update(&session_update)?;

                                for i in 0..tx_count {
                                    let transaction = Transaction::Trusting(TrustingTransaction {
                                        privacy: TrustingTransactionPrivacy::Trusting,
                                        made_at: 1700000000 + i as i64,
                                        changes: generate_transaction_json(i),
                                        meta: None,
                                    });
                                    tx.add_transaction(session_row_id, i as u64, &transaction)?;
                                }
                                Ok(())
                            })
                            .unwrap();
                    },
                );
            },
        );

        // SQLite
        group.bench_with_input(
            BenchmarkId::new("sqlite", tx_count),
            &tx_count,
            |b, &tx_count| {
                b.iter_with_setup(
                    || {
                        let storage = SqliteStorage::in_memory().unwrap();
                        let row_id = storage
                            .upsert_covalue("co_test", Some(r#"{"type":"comap"}"#))
                            .unwrap();
                        (storage, row_id)
                    },
                    |(storage, covalue_row_id)| {
                        let session_row_id = storage
                            .add_session(covalue_row_id, "session_bench", tx_count as i64 - 1, "sig_bench")
                            .unwrap();

                        for i in 0..tx_count {
                            let tx_json = generate_transaction_json(i);
                            let _ = storage.add_transaction(session_row_id, i as i64, &tx_json);
                        }
                    },
                );
            },
        );
    }

    group.finish();
}

/// Benchmark range queries (get transactions in range)
fn bench_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");
    let total_txs = 10000;
    let range_sizes = [10, 100, 1000];

    // Setup BTreeStorage with data
    let btree_storage = BTreeStorage::new(BTreeConfig::default());
    let header = generate_header();
    let covalue_row_id = btree_storage.upsert_covalue("co_test", Some(&header)).unwrap();

    btree_storage
        .transaction(|tx| {
            let session_update = SessionUpdate {
                session_update: SessionRow {
                    covalue: covalue_row_id,
                    session_id: "session_range".to_string(),
                    last_idx: total_txs as u64 - 1,
                    last_signature: "sig".to_string(),
                    bytes_since_last_signature: None,
                },
                session_row: None,
            };
            let session_row_id = tx.add_session_update(&session_update)?;

            for i in 0..total_txs {
                let transaction = Transaction::Trusting(TrustingTransaction {
                    privacy: TrustingTransactionPrivacy::Trusting,
                    made_at: 1700000000 + i as i64,
                    changes: generate_transaction_json(i),
                    meta: None,
                });
                tx.add_transaction(session_row_id, i as u64, &transaction)?;
            }
            Ok(())
        })
        .unwrap();

    // Setup SQLite with data
    let sqlite_storage = SqliteStorage::in_memory().unwrap();
    let sqlite_covalue_id = sqlite_storage
        .upsert_covalue("co_test", Some(r#"{"type":"comap"}"#))
        .unwrap();
    let sqlite_session_id = sqlite_storage
        .add_session(sqlite_covalue_id, "session_range", total_txs as i64 - 1, "sig")
        .unwrap();

    for i in 0..total_txs {
        let tx_json = generate_transaction_json(i);
        let _ = sqlite_storage.add_transaction(sqlite_session_id, i as i64, &tx_json);
    }

    // Get the session row ID for btree
    let sessions = btree_storage.get_covalue_sessions(covalue_row_id);
    let btree_session_row_id = sessions[0].row_id;

    for range_size in range_sizes {
        group.throughput(Throughput::Elements(range_size as u64));

        // BTreeStorage
        group.bench_with_input(
            BenchmarkId::new("btree", range_size),
            &range_size,
            |b, &range_size| {
                let mut rng = rand::thread_rng();
                b.iter(|| {
                    let start = rng.gen_range(0..(total_txs - range_size));
                    let _ = btree_storage.get_new_transaction_in_session(
                        black_box(btree_session_row_id),
                        black_box(start as u64),
                        black_box((start + range_size) as u64),
                    );
                });
            },
        );

        // SQLite
        group.bench_with_input(
            BenchmarkId::new("sqlite", range_size),
            &range_size,
            |b, &range_size| {
                let mut rng = rand::thread_rng();
                b.iter(|| {
                    let start = rng.gen_range(0..(total_txs - range_size));
                    let _ = sqlite_storage.get_transactions(
                        black_box(sqlite_session_id),
                        black_box(start as i64),
                        black_box((start + range_size) as i64),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Benchmark mixed workload (simulating realistic CRDT sync)
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");

    // Simulate a realistic sync operation:
    // 1. Create/lookup CoValue
    // 2. Get sessions
    // 3. Get new transactions
    // 4. Write new transactions

    let num_ops = 100;
    group.throughput(Throughput::Elements(num_ops as u64));

    // BTreeStorage
    group.bench_function("btree", |b| {
        b.iter_with_setup(
            || {
                let storage = BTreeStorage::new(BTreeConfig::default());
                let header = generate_header();
                // Pre-populate with some data
                for i in 0..100 {
                    let id = generate_covalue_id(i);
                    let row_id = storage.upsert_covalue(&id, Some(&header)).unwrap();
                    storage
                        .transaction(|tx| {
                            let session_update = SessionUpdate {
                                session_update: SessionRow {
                                    covalue: row_id,
                                    session_id: "session_0".to_string(),
                                    last_idx: 9,
                                    last_signature: "sig".to_string(),
                                    bytes_since_last_signature: None,
                                },
                                session_row: None,
                            };
                            let sess_id = tx.add_session_update(&session_update)?;
                            for j in 0..10 {
                                let transaction = Transaction::Trusting(TrustingTransaction {
                                    privacy: TrustingTransactionPrivacy::Trusting,
                                    made_at: 1700000000 + j as i64,
                                    changes: generate_transaction_json(j),
                                    meta: None,
                                });
                                tx.add_transaction(sess_id, j as u64, &transaction)?;
                            }
                            Ok(())
                        })
                        .unwrap();
                }
                storage
            },
            |storage| {
                let mut rng = rand::thread_rng();
                for _ in 0..num_ops {
                    let idx = rng.gen_range(0..100);
                    let id = generate_covalue_id(idx);

                    // Read CoValue
                    if let Some(cv) = storage.get_covalue(&id) {
                        // Get sessions
                        let sessions = storage.get_covalue_sessions(cv.row_id);
                        if !sessions.is_empty() {
                            // Read some transactions
                            let _ = storage.get_new_transaction_in_session(
                                sessions[0].row_id,
                                5,
                                10,
                            );
                        }
                    }
                }
            },
        );
    });

    // SQLite
    group.bench_function("sqlite", |b| {
        b.iter_with_setup(
            || {
                let storage = SqliteStorage::in_memory().unwrap();
                let header_json = r#"{"type":"comap"}"#;
                // Pre-populate with some data
                for i in 0..100 {
                    let id = generate_covalue_id(i);
                    let row_id = storage.upsert_covalue(&id, Some(header_json)).unwrap();
                    let sess_id = storage.add_session(row_id, "session_0", 9, "sig").unwrap();
                    for j in 0..10 {
                        let tx_json = generate_transaction_json(j);
                        let _ = storage.add_transaction(sess_id, j as i64, &tx_json);
                    }
                }
                storage
            },
            |storage| {
                let mut rng = rand::thread_rng();
                for _ in 0..num_ops {
                    let idx = rng.gen_range(0..100);
                    let id = generate_covalue_id(idx);

                    // Read CoValue
                    if let Some((cv_id, _)) = storage.get_covalue(&id).ok().flatten() {
                        // Get sessions
                        if let Ok(sessions) = storage.get_sessions(cv_id) {
                            if !sessions.is_empty() {
                                // Read some transactions
                                let _ = storage.get_transactions(sessions[0].0, 5, 10);
                            }
                        }
                    }
                }
            },
        );
    });

    group.finish();
}

/// Benchmark file-based persistence (disk I/O comparison)
fn bench_disk_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_write");
    group.sample_size(20); // Fewer samples for disk I/O tests

    let num_covalues = 1000;
    group.throughput(Throughput::Elements(num_covalues as u64));

    // BTreeStorage with file-based StdFileIO
    // Note: BTreeStorage currently uses in-memory BTreeMap
    // This benchmark shows the baseline for when file backing is added
    group.bench_function("btree_memory", |b| {
        b.iter_with_setup(
            || BTreeStorage::new(BTreeConfig::default()),
            |storage| {
                let header = generate_header();
                for i in 0..num_covalues {
                    let id = generate_covalue_id(i);
                    storage.upsert_covalue(&id, Some(&header));
                }
            },
        );
    });

    // SQLite with file-based storage
    group.bench_function("sqlite_disk", |b| {
        let dir = tempdir().unwrap();
        b.iter_with_setup(
            || {
                let db_path = dir.path().join("bench.db");
                SqliteStorage::new(db_path.to_str().unwrap()).unwrap()
            },
            |storage| {
                let header_json = r#"{"type":"comap"}"#;
                for i in 0..num_covalues {
                    let id = generate_covalue_id(i);
                    let _ = storage.upsert_covalue(&id, Some(header_json));
                }
            },
        );
    });

    // SQLite in-memory for comparison
    group.bench_function("sqlite_memory", |b| {
        b.iter_with_setup(
            || SqliteStorage::in_memory().unwrap(),
            |storage| {
                let header_json = r#"{"type":"comap"}"#;
                for i in 0..num_covalues {
                    let id = generate_covalue_id(i);
                    let _ = storage.upsert_covalue(&id, Some(header_json));
                }
            },
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_write,
    bench_bulk_write,
    bench_single_read,
    bench_transaction_write,
    bench_range_query,
    bench_mixed_workload,
    bench_disk_write,
);
criterion_main!(benches);

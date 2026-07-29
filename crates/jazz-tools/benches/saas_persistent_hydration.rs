//! Persistent serving-database hydration benchmark for the SaaS permission fixture.
//!
//! This deliberately uses the same durable stack as the server:
//! `RocksDbStorage::open` (WAL, no per-commit fsync) plus
//! `Db::open_history_complete`. By default it seeds a temporary database,
//! closes it cleanly, reopens it with a fresh RocksDB block cache, and then
//! measures initial subscription hydration at the Global tier.
//!
//! A reopen does not evict the operating-system page cache. The emitted cache
//! caveat and RocksDB block-read counters make that limitation explicit.

mod saas_fanout_fixture;
mod saas_fanout_oracle;
mod saas_permission_support;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionStream, block_on,
};
use jazz::groove::db::{StorageReadBucket, StorageReadMetrics};
use jazz::groove::records::Value;
use jazz::groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::query::{OrderDirection, Query, col, eq, in_list, lit, param};
use jazz::schema::JazzSchema;
use jazz::tx::DurabilityTier;
use rocksdb::perf::set_perf_stats;
use rocksdb::{PerfContext, PerfMetric, PerfStatsLevel};
use saas_fanout_fixture::{AccessPath, Config, Fixture, SeedReport};
use saas_fanout_oracle::take_initial_reset;
use serde::Serialize;
use serde_json::json;

type BenchDb = Db<RocksDbStorage>;

fn main() {
    match run() {
        Ok(output) => println!(
            "{}",
            serde_json::to_string_pretty(&output).expect("serialize persistent hydration output")
        ),
        Err(error) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "benchmark": "saas_persistent_hydration",
                    "completed": false,
                    "ok": false,
                    "error": error,
                }))
                .expect("serialize persistent hydration error")
            );
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkOutput {
    benchmark: &'static str,
    completed: bool,
    ok: bool,
    config: Config,
    storage: &'static str,
    durability: &'static str,
    read_tier: &'static str,
    reopened_after_seed: bool,
    cache_state: CacheStateReport,
    temporary_data_dir_bytes: u64,
    seed_open: OpenReport,
    seeding: SeedReport,
    measurement_open: OpenReport,
    subscriptions: Vec<SubscriptionReport>,
    total_prepare_ms: f64,
    total_subscribe_ms: f64,
    exact_initial_membership: bool,
    tooling_friction: &'static str,
}

#[derive(Debug, Serialize)]
struct CacheStateReport {
    rocksdb_block_cache: &'static str,
    os_page_cache: &'static str,
    cold_disk_claimed: bool,
    caveat: &'static str,
}

#[derive(Debug, Serialize)]
struct OpenReport {
    storage_open_ms: f64,
    db_recovery_ms: f64,
    recovery_logical_storage: LogicalStorageReadReport,
    recovery_rocksdb: RocksReadReport,
}

impl OpenReport {
    fn reused_seed_open() -> Self {
        Self {
            storage_open_ms: 0.0,
            db_recovery_ms: 0.0,
            recovery_logical_storage: LogicalStorageReadReport::default(),
            recovery_rocksdb: RocksReadReport::default(),
        }
    }
}

#[derive(Debug, Serialize)]
struct SubscriptionReport {
    subscription_index: usize,
    access_path: AccessPath,
    team_index: usize,
    documents_in_team: usize,
    prepare_ms: f64,
    subscribe_ms: f64,
    initial_rows: usize,
    logical_storage: LogicalStorageReadReport,
    rocksdb: RocksReadReport,
    exact_initial_reset: bool,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct LogicalStorageReadBucketReport {
    reads: usize,
    ranges: usize,
}

impl From<StorageReadBucket> for LogicalStorageReadBucketReport {
    fn from(bucket: StorageReadBucket) -> Self {
        Self {
            reads: bucket.reads,
            ranges: bucket.ranges,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct LogicalStorageReadReport {
    total: LogicalStorageReadBucketReport,
    history_rows: LogicalStorageReadBucketReport,
    history_indexes: LogicalStorageReadBucketReport,
    global_current_rows: LogicalStorageReadBucketReport,
    global_current_indexes: LogicalStorageReadBucketReport,
    register_global_current_rows: LogicalStorageReadBucketReport,
    global_changes_rows: LogicalStorageReadBucketReport,
    global_changes_indexes: LogicalStorageReadBucketReport,
    transactions_rows: LogicalStorageReadBucketReport,
    transactions_indexes: LogicalStorageReadBucketReport,
    other: LogicalStorageReadBucketReport,
}

impl From<StorageReadMetrics> for LogicalStorageReadReport {
    fn from(metrics: StorageReadMetrics) -> Self {
        Self {
            total: metrics.total.into(),
            history_rows: metrics.history_rows.into(),
            history_indexes: metrics.history_indexes.into(),
            global_current_rows: metrics.global_current_rows.into(),
            global_current_indexes: metrics.global_current_indexes.into(),
            register_global_current_rows: metrics.register_global_current_rows.into(),
            global_changes_rows: metrics.global_changes_rows.into(),
            global_changes_indexes: metrics.global_changes_indexes.into(),
            transactions_rows: metrics.transactions_rows.into(),
            transactions_indexes: metrics.transactions_indexes.into(),
            other: metrics.other.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct RocksReadReport {
    block_cache_hits: u64,
    block_reads: u64,
    block_read_bytes: u64,
    block_read_time_ns: u64,
    iterator_read_bytes: u64,
    internal_keys_skipped: u64,
}

impl RocksReadReport {
    fn capture(context: &PerfContext) -> Self {
        Self {
            block_cache_hits: context.metric(PerfMetric::BlockCacheHitCount),
            block_reads: context.metric(PerfMetric::BlockReadCount),
            block_read_bytes: context.metric(PerfMetric::BlockReadByte),
            block_read_time_ns: context.metric(PerfMetric::BlockReadTime),
            iterator_read_bytes: context.metric(PerfMetric::IterReadBytes),
            internal_keys_skipped: context.metric(PerfMetric::InternalKeySkippedCount),
        }
    }
}

fn run() -> Result<BenchmarkOutput, String> {
    let config = Config::from_env()?;
    let fixture = Fixture::build(config.clone())?;
    let reopen_after_seed = env_bool("JAZZ_SAAS_PERSIST_REOPEN", true)?;
    let temp_dir = tempfile::tempdir()
        .map_err(|error| format!("create temporary RocksDB directory: {error}"))?;

    set_perf_stats(PerfStatsLevel::EnableTimeExceptForMutex);

    let (seed_db, seed_open) = open_db(temp_dir.path(), fixture.schema())?;
    eprintln!(
        "seeding persistent fixture documents={}, teams={}, active_subscriptions={}",
        config.documents, config.teams, config.active_subscriptions
    );
    // Unlike `seed_local`, this finalizes every batch into the Global view used
    // by a history-complete serving core.
    let seeding = fixture.seed_global(&seed_db)?;

    let (db, measurement_open) = if reopen_after_seed {
        seed_db
            .close()
            .map_err(|error| format!("cleanly close seeded database: {error}"))?;
        drop(seed_db);
        open_db(temp_dir.path(), fixture.schema())?
    } else {
        (seed_db, OpenReport::reused_seed_open())
    };

    let query = document_list_query();
    let read_opts = global_read_opts();
    let mut streams = Vec::<SubscriptionStream>::with_capacity(config.active_subscriptions);
    let mut reports = Vec::with_capacity(config.active_subscriptions);
    let mut total_prepare = Duration::ZERO;
    let mut total_subscribe = Duration::ZERO;

    for plan in fixture.subscribers() {
        db.set_identity_claims(plan.identity, plan.claims.clone());

        let prepare_started = Instant::now();
        let prepared = db
            .prepare_query_bound(
                &query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(plan.team.0))]),
            )
            .map_err(|error| format!("prepare subscription {}: {error}", plan.index))?;
        let prepare_elapsed = prepare_started.elapsed();
        total_prepare += prepare_elapsed;

        db.reset_storage_read_metrics_for_test();
        let mut perf = PerfContext::default();
        perf.reset();
        let subscribe_started = Instant::now();
        let mut stream =
            block_on(db.subscribe_for_identity(&prepared, read_opts.clone(), plan.identity))
                .map_err(|error| format!("subscribe {}: {error}", plan.index))?;
        let subscribe_elapsed = subscribe_started.elapsed();
        total_subscribe += subscribe_elapsed;
        let logical_reads = db.take_storage_read_metrics_for_test();
        let rocksdb = RocksReadReport::capture(&perf);

        let initial = take_initial_reset(
            &format!("persistent-subscription-{}", plan.index),
            &mut stream,
            plan.expected_page(),
        )?;
        reports.push(SubscriptionReport {
            subscription_index: plan.index,
            access_path: plan.access_path,
            team_index: plan.team_index,
            documents_in_team: fixture.distribution().count(plan.team_index),
            prepare_ms: millis(prepare_elapsed),
            subscribe_ms: millis(subscribe_elapsed),
            initial_rows: initial.observed.len(),
            logical_storage: logical_reads.into(),
            rocksdb,
            exact_initial_reset: true,
        });
        streams.push(stream);
    }

    drop(streams);
    db.close()
        .map_err(|error| format!("cleanly close measured database: {error}"))?;
    drop(db);
    let temporary_data_dir_bytes = directory_bytes(temp_dir.path())?;

    Ok(BenchmarkOutput {
        benchmark: "saas_persistent_hydration",
        completed: true,
        ok: true,
        config,
        storage: "RocksDbStorage::open",
        durability: "wal_no_sync (server durable-storage default)",
        read_tier: "global",
        reopened_after_seed: reopen_after_seed,
        cache_state: CacheStateReport {
            rocksdb_block_cache: if reopen_after_seed {
                "fresh instance after reopen"
            } else {
                "same instance used for seed and measurement"
            },
            os_page_cache: "uncontrolled; likely warm after same-process seed",
            cold_disk_claimed: false,
            caveat: "RocksDB block reads may be served by the operating-system page cache; this benchmark is a persistent-backend/reopen measurement, not a cold-disk claim.",
        },
        temporary_data_dir_bytes,
        seed_open,
        seeding,
        measurement_open,
        subscriptions: reports,
        total_prepare_ms: millis(total_prepare),
        total_subscribe_ms: millis(total_subscribe),
        exact_initial_membership: true,
        tooling_friction: "A reusable pre-seeded data directory plus a dedicated cache-controlled benchmark host is required for honest cold-disk samples.",
    })
}

fn open_db(path: &Path, schema: JazzSchema) -> Result<(BenchDb, OpenReport), String> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    let storage_started = Instant::now();
    let rocks = RocksDbStorage::open(path, &refs)
        .map_err(|error| format!("open benchmark RocksDB storage: {error}"))?;
    let storage_elapsed = storage_started.elapsed();

    let mut perf = PerfContext::default();
    perf.reset();
    let recovery_started = Instant::now();
    let db = block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            rocks,
            DbIdentity {
                node: NodeUuid::from_bytes([0x5e; 16]),
                author: AuthorId::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x5e)),
    ))
    .map_err(|error| format!("open history-complete benchmark db: {error}"))?;
    let recovery_elapsed = recovery_started.elapsed();
    let recovery_reads = db.take_storage_read_metrics_for_test();
    let recovery_rocksdb = RocksReadReport::capture(&perf);

    Ok((
        db,
        OpenReport {
            storage_open_ms: millis(storage_elapsed),
            db_recovery_ms: millis(recovery_elapsed),
            recovery_logical_storage: recovery_reads.into(),
            recovery_rocksdb,
        },
    ))
}

fn global_read_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn document_list_query() -> Query {
    Query::from(saas_permission_support::DOCUMENTS)
        .filter(eq(col("team"), param("team")))
        .filter(eq(col("archived"), lit(false)))
        .filter(in_list(col("status"), [lit("active"), lit("draft")]))
        .order_by("updated_at", OrderDirection::Desc)
        .order_by("id", OrderDirection::Desc)
        .limit(saas_fanout_oracle::TOP_PAGE_SIZE)
}

fn env_bool(key: &str, default: bool) -> Result<bool, String> {
    match std::env::var(key) {
        Ok(value) => match value.as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => Err(format!("{key} must be true/false or 1/0, got {value:?}")),
        },
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("read {key}: {error}")),
    }
}

fn directory_bytes(path: &Path) -> Result<u64, String> {
    let mut total = 0_u64;
    let entries =
        std::fs::read_dir(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("stat {}: {error}", entry.path().display()))?;
        if metadata.is_dir() {
            total = total
                .checked_add(directory_bytes(&entry.path())?)
                .ok_or_else(|| "temporary data directory byte count overflowed".to_owned())?;
        } else {
            total = total
                .checked_add(metadata.len())
                .ok_or_else(|| "temporary data directory byte count overflowed".to_owned())?;
        }
    }
    Ok(total)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

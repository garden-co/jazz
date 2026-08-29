//! Persistent Global-query hydration receipt with fixed selectivity.
//!
//! The selected team and result size stay fixed while total table size grows.
//! Logical storage-read counters reveal whether initial query hydration follows
//! the declared `team` index or scans the entire Global current table.
//!
//! ```text
//! cargo bench -p jazz --features testing --bench selective_global_hydration --quiet
//! ```
//!
//! The default run exposes fixed 10k and 100k wall-time benchmarks to Divan
//! and CodSpeed. Set `JAZZ_SELECTIVE_HYDRATION_RECEIPT=1` to run the JSONL
//! scale receipt instead; in that mode `JAZZ_SELECTIVE_HYDRATION_ROWS`
//! controls the comma-separated table-size ladder and
//! `JAZZ_SELECTIVE_HYDRATION_TARGET_ROWS`,
//! `JAZZ_SELECTIVE_HYDRATION_RESULT_ROWS`, and
//! `JAZZ_SELECTIVE_HYDRATION_BATCH_ROWS` control selection and seed batching.

mod schema_fixture;
mod support;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, MergeableTxOps, PreparedQuery, Propagation, ReadOpts,
    SeededRowIdSource, SubscriptionEvent, block_on,
};
use jazz::groove::db::StorageReadMetrics;
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit, param};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_storage_rocksdb::RocksDbStorage;
use serde_json::{Map, json};
use sha2::{Digest, Sha256};

const TABLE: &str = "documents";

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();

    // CodSpeed recognizes Divan's benchmark protocol, not the JSONL receipt
    // protocol below. Keep the latter for the 1M-row diagnostic rung, but make
    // the normal bench target real wall-time benchmarks.
    if std::env::var_os("JAZZ_SELECTIVE_HYDRATION_RECEIPT").is_some() {
        run_receipt();
        return;
    }

    divan::main();
}

/// Run the full read-count receipt, including the 1M diagnostic rung.
///
/// This is intentionally opt-in: it proves the selectivity invariant and is
/// much too expensive to be a repeated CodSpeed sample.
fn run_receipt() {
    let config = Config::from_env();
    config.validate();

    let mut receipts = Vec::with_capacity(config.table_rows.len());
    for &table_rows in &config.table_rows {
        let receipt = run_rung(config.as_ref(), table_rows);
        receipt.emit();
        receipts.push(receipt);
    }

    let first = receipts.first().expect("non-empty table-size ladder");
    let last = receipts.last().expect("non-empty table-size ladder");
    let mut fields = Map::new();
    fields.insert("phase".to_owned(), json!("scale_summary"));
    fields.insert("selection_fixed".to_owned(), json!(true));
    fields.insert("first_table_rows".to_owned(), json!(first.table_rows));
    fields.insert("last_table_rows".to_owned(), json!(last.table_rows));
    fields.insert(
        "table_scale_ratio".to_owned(),
        json!(last.table_rows as f64 / first.table_rows as f64),
    );
    fields.insert(
        "query_path_global_current_row_read_ratio".to_owned(),
        json!(ratio(
            query_path_global_row_reads(last),
            query_path_global_row_reads(first),
        )),
    );
    fields.insert(
        "query_path_global_current_index_read_ratio".to_owned(),
        json!(ratio(
            query_path_global_index_reads(last),
            query_path_global_index_reads(first),
        )),
    );
    fields.insert(
        "end_to_end_global_current_row_read_ratio".to_owned(),
        json!(ratio(
            end_to_end_global_row_reads(last),
            end_to_end_global_row_reads(first),
        )),
    );
    fields.insert(
        "end_to_end_global_current_index_read_ratio".to_owned(),
        json!(ratio(
            end_to_end_global_index_reads(last),
            end_to_end_global_index_reads(first),
        )),
    );
    fields.insert(
        "tooling_friction".to_owned(),
        json!("A reusable settled fixture would avoid reseeding each scale rung while preserving the fixed-selection read boundary."),
    );
    support::emit_json_line("selective_global_hydration", fields);
}

/// CodSpeed wall-time receipt for the maintained initial hydration path at a
/// fixed 10k-row table size. Seeding, reopening, query preparation, and one
/// fully-asserted validation pass are deliberately outside the timed closure.
#[divan::bench(sample_count = 10)]
fn maintained_subscription_hydration_10k(bencher: divan::Bencher<'_, '_>) {
    benchmark_maintained_subscription_hydration(bencher, 10_000);
}

/// The same fixed-selectivity workload at 100k rows. Fewer samples retain a
/// useful hosted receipt without turning fixture setup into the dominant CI
/// cost. The 1M rung remains available through `run_receipt` above.
#[divan::bench(sample_count = 3)]
fn maintained_subscription_hydration_100k(bencher: divan::Bencher<'_, '_>) {
    benchmark_maintained_subscription_hydration(bencher, 100_000);
}

fn benchmark_maintained_subscription_hydration(bencher: divan::Bencher<'_, '_>, table_rows: usize) {
    let fixture = HydrationFixture::new(table_rows);
    let baseline_subscriptions = fixture.active_groove_subscriptions();

    // Keep the correctness/read-bound proof out of the measurement. The timed
    // operation below is exactly the same initial maintained hydration.
    fixture.assert_selective_hydration();
    fixture.assert_subscription_baseline(baseline_subscriptions, "validation hydration");

    bencher.bench_local(|| divan::black_box(fixture.hydrate_maintained()));
    // `SubscriptionStream::drop` only queues its finalizer so it can never
    // block a caller suspended on storage. Divan drops each timed result, but
    // does not run Jazz's owner turn for us; drain that queued work outside
    // the timing boundary before this fixture is released or reused.
    fixture.assert_subscription_baseline(baseline_subscriptions, "Divan samples");
}

#[derive(Clone, Copy)]
struct ConfigRef {
    target_rows: usize,
    result_rows: usize,
    batch_rows: usize,
}

struct Config {
    table_rows: Vec<usize>,
    target_rows: usize,
    result_rows: usize,
    batch_rows: usize,
}

impl Config {
    fn from_env() -> Self {
        Self {
            table_rows: support::csv_usizes(
                "JAZZ_SELECTIVE_HYDRATION_ROWS",
                "10000,100000,1000000",
            ),
            target_rows: support::env_usize("JAZZ_SELECTIVE_HYDRATION_TARGET_ROWS", 100),
            result_rows: support::env_usize("JAZZ_SELECTIVE_HYDRATION_RESULT_ROWS", 50),
            batch_rows: support::env_usize("JAZZ_SELECTIVE_HYDRATION_BATCH_ROWS", 1000),
        }
    }

    fn validate(&self) {
        assert!(
            !self.table_rows.is_empty(),
            "table-size ladder must not be empty"
        );
        assert!(self.target_rows > 0, "target row count must be positive");
        assert!(self.result_rows > 0, "result row count must be positive");
        assert!(self.batch_rows > 0, "seed batch size must be positive");
        assert!(
            self.result_rows <= self.target_rows,
            "result rows must not exceed target rows"
        );
        assert!(
            self.table_rows.iter().all(|rows| *rows >= self.target_rows),
            "every table-size rung must contain the fixed target rows"
        );
        assert!(
            self.table_rows.windows(2).all(|pair| pair[0] < pair[1]),
            "table-size ladder must be strictly increasing"
        );
    }

    fn as_ref(&self) -> ConfigRef {
        ConfigRef {
            target_rows: self.target_rows,
            result_rows: self.result_rows,
            batch_rows: self.batch_rows,
        }
    }
}

struct RungReceipt {
    table_rows: usize,
    target_rows: usize,
    result_rows: usize,
    seed_us: u128,
    storage_open_us: u128,
    db_open_us: u128,
    prepare_us: u128,
    query_us: u128,
    maintained_subscribe_us: u128,
    result_digest: String,
    maintained_result_digest: String,
    open_metrics: StorageReadMetrics,
    prepare_metrics: StorageReadMetrics,
    query_metrics: StorageReadMetrics,
    maintained_metrics: StorageReadMetrics,
}

/// A settled RocksDB database plus its prepared fixed-selectivity query.
///
/// The tempdir is retained for the lifetime of the database so every timed
/// sample reads the same reopened persisted fixture. Each benchmark invocation
/// constructs this once, outside Divan's timed closure.
struct HydrationFixture {
    _temp: tempfile::TempDir,
    db: Db<RocksDbStorage>,
    prepared: PreparedQuery,
    expected: Vec<RowUuid>,
    target_rows: usize,
}

struct HydrationMeasurement {
    row_count: usize,
    result_digest: String,
    metrics: StorageReadMetrics,
}

impl HydrationFixture {
    fn new(table_rows: usize) -> Self {
        let config = ConfigRef {
            target_rows: 100,
            result_rows: 50,
            batch_rows: 1_000,
        };
        let temp = tempfile::tempdir().expect("create selective-hydration RocksDB directory");
        let schema = schema();
        let (seed_db, _, _) = open_db(temp.path(), schema.clone());
        seed_rows(&seed_db, config, table_rows);
        block_on(seed_db.close()).expect("close seeded selective-hydration database");
        drop(seed_db);

        let (db, _, _) = open_db(temp.path(), schema);
        let query = selective_query(config.result_rows);
        let prepared = db
            .prepare_query_bound(
                &query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(target_team().0))]),
            )
            .expect("prepare selective Global query");
        let expected = (config.target_rows - config.result_rows..config.target_rows)
            .rev()
            .map(target_row)
            .collect();

        Self {
            _temp: temp,
            db,
            prepared,
            expected,
            target_rows: config.target_rows,
        }
    }

    fn assert_selective_hydration(&self) {
        self.db.reset_storage_read_metrics_for_test();
        let rows = block_on(self.db.all_for_identity(
            &self.prepared,
            global_read_opts(),
            AuthorSubject::SYSTEM,
        ))
        .expect("run selective Global query");
        let observed = rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
        assert_eq!(observed, self.expected, "selective Global result changed");
        let query_metrics = self.db.take_storage_read_metrics_for_test();
        assert_selective_metrics(&query_metrics, self.target_rows, "query");

        let measured = self.hydrate_maintained();
        assert_eq!(measured.row_count, self.expected.len());
        assert_eq!(measured.result_digest, digest_rows(&self.expected));
        assert_selective_metrics(&measured.metrics, self.target_rows, "maintained");
    }

    fn active_groove_subscriptions(&self) -> usize {
        self.db.active_groove_subscriptions_for_test()
    }

    /// Drain the non-blocking `SubscriptionStream::drop` finalizers and prove
    /// a repeated benchmark cannot retain a Groove graph per sample.
    ///
    /// This must remain outside `hydrate_maintained`: the timed receipt is the
    /// opening/reset path, while finalization is an ordinary later owner turn.
    fn assert_subscription_baseline(&self, baseline: usize, phase: &str) {
        block_on(self.db.tick()).expect("drain queued maintained-subscription finalizers");
        assert_eq!(
            self.active_groove_subscriptions(),
            baseline,
            "{phase} must retire every dropped maintained Groove subscription"
        );
    }

    /// The timed operation: create one maintained subscription and consume its
    /// initial reset. It returns the same result digest and logical read
    /// counters asserted by `assert_selective_hydration`, but performs no
    /// assertion inside the wall-time sample.
    fn hydrate_maintained(&self) -> HydrationMeasurement {
        self.db.reset_storage_read_metrics_for_test();
        let mut subscription = block_on(self.db.subscribe(&self.prepared, local_read_opts()))
            .expect("open selective maintained subscription");
        let SubscriptionEvent::Delta {
            reset: true, added, ..
        } = subscription
            .try_next_event()
            .expect("maintained subscription must emit its initial reset")
        else {
            panic!("maintained subscription must emit an initial reset");
        };
        let rows = added
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        let metrics = self.db.take_storage_read_metrics_for_test();
        // This only queues teardown. The caller deliberately runs `Db::tick`
        // after the timed operation, where the owner can retire the retained
        // Groove subscription without contaminating the sample.
        drop(subscription);
        HydrationMeasurement {
            row_count: rows.len(),
            result_digest: digest_rows(&rows),
            metrics,
        }
    }
}

fn selective_query(result_rows: usize) -> Query {
    Query::from(TABLE)
        .filter(eq(col("team"), param("team")))
        .filter(eq(col("active"), lit(true)))
        .order_by("updated_at", OrderDirection::Desc)
        .order_by("id", OrderDirection::Desc)
        .limit(result_rows)
}

fn assert_selective_metrics(metrics: &StorageReadMetrics, target_rows: usize, phase: &str) {
    assert!(
        metrics.global_current_rows.reads <= target_rows,
        "{phase} selective Global hydration read {} current rows for {target_rows} indexed candidates",
        metrics.global_current_rows.reads,
    );
    assert!(
        (1..=target_rows).contains(&metrics.global_current_indexes.reads),
        "{phase} selective Global hydration must use the declared index without reading more than the fixed candidate set",
    );
}

impl RungReceipt {
    fn emit(&self) {
        let mut fields = Map::new();
        fields.insert("phase".to_owned(), json!("query_hydration"));
        fields.insert("storage".to_owned(), json!("rocksdb_wal_no_sync"));
        fields.insert("read_tier".to_owned(), json!("global"));
        fields.insert(
            "cache_state".to_owned(),
            json!("fresh_rocksdb_instance_os_cache_uncontrolled"),
        );
        fields.insert("table_rows".to_owned(), json!(self.table_rows));
        fields.insert("target_rows".to_owned(), json!(self.target_rows));
        fields.insert("result_rows".to_owned(), json!(self.result_rows));
        fields.insert("selection_fixed".to_owned(), json!(true));
        fields.insert("result_digest".to_owned(), json!(self.result_digest));
        fields.insert("seed_us".to_owned(), json!(self.seed_us));
        fields.insert("storage_open_us".to_owned(), json!(self.storage_open_us));
        fields.insert("db_open_us".to_owned(), json!(self.db_open_us));
        fields.insert("prepare_us".to_owned(), json!(self.prepare_us));
        fields.insert("query_us".to_owned(), json!(self.query_us));
        fields.insert(
            "maintained_subscribe_us".to_owned(),
            json!(self.maintained_subscribe_us),
        );
        fields.insert(
            "maintained_result_digest".to_owned(),
            json!(self.maintained_result_digest),
        );
        insert_read_metrics(&mut fields, "open", &self.open_metrics);
        insert_read_metrics(&mut fields, "prepare", &self.prepare_metrics);
        insert_read_metrics(&mut fields, "query", &self.query_metrics);
        insert_read_metrics(&mut fields, "maintained", &self.maintained_metrics);
        support::emit_json_line("selective_global_hydration", fields);
    }
}

fn run_rung(config: ConfigRef, table_rows: usize) -> RungReceipt {
    let temp = tempfile::tempdir().expect("create selective-hydration RocksDB directory");
    let schema = schema();
    let (db, _, _) = open_db(temp.path(), schema.clone());

    let seed_started = Instant::now();
    seed_rows(&db, config, table_rows);
    let seed_us = seed_started.elapsed().as_micros();
    block_on(db.close()).expect("close seeded selective-hydration database");
    drop(db);

    let (db, storage_open_us_after_seed, db_open_us_after_seed) = open_db(temp.path(), schema);
    let open_metrics = db.take_storage_read_metrics_for_test();
    let query = Query::from(TABLE)
        .filter(eq(col("team"), param("team")))
        .filter(eq(col("active"), lit(true)))
        .order_by("updated_at", OrderDirection::Desc)
        .order_by("id", OrderDirection::Desc)
        .limit(config.result_rows);
    db.reset_storage_read_metrics_for_test();
    let prepare_started = Instant::now();
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(target_team().0))]),
        )
        .expect("prepare selective Global query");
    let prepare_us = prepare_started.elapsed().as_micros();
    let prepare_metrics = db.take_storage_read_metrics_for_test();

    db.reset_storage_read_metrics_for_test();
    let query_started = Instant::now();
    // This receipt seeds a complete authoritative database directly, rather
    // than consuming an identity-scoped result set delivered by an upstream
    // peer. `Db::all` is deliberately the latter client-local API at Global;
    // use the serving entry point so this measures the declared index path.
    let rows = block_on(db.all_for_identity(&prepared, global_read_opts(), AuthorSubject::SYSTEM))
        .expect("run selective Global query");
    let query_us = query_started.elapsed().as_micros();
    let query_metrics = db.take_storage_read_metrics_for_test();

    assert!(
        query_metrics.global_current_rows.reads <= config.target_rows,
        "selective Global hydration read {} current rows for {} indexed candidates at table size {table_rows}",
        query_metrics.global_current_rows.reads,
        config.target_rows,
    );
    assert!(
        (1..=config.target_rows).contains(&query_metrics.global_current_indexes.reads),
        "selective Global hydration must use the declared index without reading more than the fixed candidate set"
    );

    let observed = rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
    let expected = (config.target_rows - config.result_rows..config.target_rows)
        .rev()
        .map(target_row)
        .collect::<Vec<_>>();
    assert_eq!(
        observed, expected,
        "selective Global result changed at table size {table_rows}"
    );
    let result_digest = digest_rows(&observed);
    assert_eq!(result_digest, digest_rows(&expected));

    // A maintained subscription must hydrate through the same declared index
    // and retain a live continuation, not fall back to a separate one-shot
    // authority read. The fixture is history-complete: its ahead overlay is
    // empty, so any table-size growth here is attributable to the settled
    // source selection rather than unfinalized local writes.
    db.reset_storage_read_metrics_for_test();
    let subscribe_started = Instant::now();
    let mut subscription = block_on(db.subscribe(&prepared, local_read_opts()))
        .expect("open selective maintained subscription");
    let maintained_subscribe_us = subscribe_started.elapsed().as_micros();
    let maintained_metrics = db.take_storage_read_metrics_for_test();
    let SubscriptionEvent::Delta {
        reset: true,
        added,
        updated,
        removed,
        ..
    } = subscription
        .try_next_event()
        .expect("maintained subscription must emit its initial reset")
    else {
        panic!("maintained subscription must emit an initial reset");
    };
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let maintained_rows = added
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    assert_eq!(maintained_rows, expected);
    assert!(
        maintained_metrics.global_current_rows.reads <= config.target_rows,
        "maintained selective hydration read {} current rows for {} indexed candidates at table size {table_rows}",
        maintained_metrics.global_current_rows.reads,
        config.target_rows,
    );
    assert!(
        (1..=config.target_rows).contains(&maintained_metrics.global_current_indexes.reads),
        "maintained selective hydration must use the declared index without reading more than the fixed candidate set",
    );
    let maintained_result_digest = digest_rows(&maintained_rows);
    assert_eq!(maintained_result_digest, result_digest);

    block_on(db.close()).expect("close measured selective-hydration database");

    RungReceipt {
        table_rows,
        target_rows: config.target_rows,
        result_rows: config.result_rows,
        seed_us,
        storage_open_us: storage_open_us_after_seed,
        db_open_us: db_open_us_after_seed,
        prepare_us,
        query_us,
        maintained_subscribe_us,
        result_digest,
        maintained_result_digest,
        open_metrics,
        prepare_metrics,
        query_metrics,
        maintained_metrics,
    }
}

fn schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new(TABLE)
                .column("team", ColumnType::Uuid)
                .column("active", ColumnType::Boolean)
                .column("updated_at", ColumnType::Timestamp)
                .column("title", ColumnType::Text)
                .index_only(["team"]),
        ),
    )
}

fn open_db(path: &Path, schema: JazzSchema) -> (Db<RocksDbStorage>, u128, u128) {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage_started = Instant::now();
    let storage = RocksDbStorage::open(path, &refs).expect("open selective-hydration RocksDB");
    let storage_open_us = storage_started.elapsed().as_micros();
    let db_started = Instant::now();
    let db = block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x73; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x73)),
    ))
    .expect("open selective-hydration Jazz database");
    let db_open_us = db_started.elapsed().as_micros();
    (db, storage_open_us, db_open_us)
}

fn seed_rows(db: &Db<RocksDbStorage>, config: ConfigRef, table_rows: usize) {
    for batch_start in (0..table_rows).step_by(config.batch_rows) {
        let batch_end = table_rows.min(batch_start + config.batch_rows);
        let tx = block_on(db.mergeable_tx()).expect("open selective-hydration seed transaction");
        for index in batch_start..batch_end {
            let (row, team, updated_at) = if index < config.target_rows {
                (target_row(index), target_team(), index)
            } else {
                (filler_row(index), filler_team(), index)
            };
            block_on(tx.insert(
                TABLE,
                BTreeMap::from([
                    ("team".to_owned(), Value::Uuid(team.0)),
                    ("active".to_owned(), Value::Bool(true)),
                    ("updated_at".to_owned(), Value::U64(updated_at as u64)),
                    (
                        "title".to_owned(),
                        Value::String(format!("document-{index}")),
                    ),
                ]),
                jazz::db::InsertOptions {
                    row_id: Some(row),
                    ..Default::default()
                },
            ))
            .expect("stage selective-hydration seed row");
        }
        let tx_id = block_on(tx.commit()).expect("commit selective-hydration seed batch");
        db.finalize_local_mergeable_commit_for_test(tx_id)
            .expect("settle selective-hydration seed batch");
    }
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

fn local_read_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn target_team() -> RowUuid {
    tagged_row(0x10, 1)
}

fn filler_team() -> RowUuid {
    tagged_row(0x20, 1)
}

fn target_row(index: usize) -> RowUuid {
    tagged_row(0x30, index as u64)
}

fn filler_row(index: usize) -> RowUuid {
    tagged_row(0x40, index as u64)
}

fn tagged_row(tag: u8, index: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = tag;
    bytes[8..].copy_from_slice(&index.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn digest_rows(rows: &[RowUuid]) -> String {
    let mut digest = Sha256::new();
    for row in rows {
        digest.update(row.0.as_bytes());
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn insert_read_metrics(
    fields: &mut Map<String, serde_json::Value>,
    prefix: &str,
    metrics: &StorageReadMetrics,
) {
    fields.insert(
        format!("{prefix}_logical_reads"),
        json!(metrics.total.reads),
    );
    fields.insert(
        format!("{prefix}_logical_ranges"),
        json!(metrics.total.ranges),
    );
    fields.insert(
        format!("{prefix}_global_current_row_reads"),
        json!(metrics.global_current_rows.reads),
    );
    fields.insert(
        format!("{prefix}_global_current_row_ranges"),
        json!(metrics.global_current_rows.ranges),
    );
    fields.insert(
        format!("{prefix}_global_current_index_reads"),
        json!(metrics.global_current_indexes.reads),
    );
    fields.insert(
        format!("{prefix}_global_current_index_ranges"),
        json!(metrics.global_current_indexes.ranges),
    );
}

fn query_path_global_row_reads(receipt: &RungReceipt) -> usize {
    receipt.prepare_metrics.global_current_rows.reads
        + receipt.query_metrics.global_current_rows.reads
}

fn query_path_global_index_reads(receipt: &RungReceipt) -> usize {
    receipt.prepare_metrics.global_current_indexes.reads
        + receipt.query_metrics.global_current_indexes.reads
}

fn end_to_end_global_row_reads(receipt: &RungReceipt) -> usize {
    receipt.open_metrics.global_current_rows.reads + query_path_global_row_reads(receipt)
}

fn end_to_end_global_index_reads(receipt: &RungReceipt) -> usize {
    receipt.open_metrics.global_current_indexes.reads + query_path_global_index_reads(receipt)
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    (denominator != 0).then(|| numerator as f64 / denominator as f64)
}

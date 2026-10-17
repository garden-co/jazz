//! A label's live release view over a huge, persisted multi-tenant table: one
//! maintained subscription whose initial hydration must follow the declared
//! `label` index instead of scanning every tenant's releases.
//!
//! Moved from `crates/jazz/benches/selective_global_hydration.rs`
//! (`maintained_subscription_hydration_100k`), reframed in BigLabel terms:
//! documents are releases, the selected team is one label, the filler team is
//! every other tenant. Fixture, query shape, timed work and read-bound
//! assertions are unchanged. The crate bench keeps the JSONL scale receipt
//! (`JAZZ_SELECTIVE_HYDRATION_RECEIPT`, including the 1M rung) for diagnosis.

use std::collections::BTreeMap;
use std::path::Path;

use jazz::db::{
    Db, DbConfig, DbIdentity, MergeableTxOps, PreparedQuery, ReadOpts, ReadTier, SeededRowIdSource,
    SubscriptionEvent, block_on,
};
use jazz::groove::db::StorageReadMetrics;
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit, param};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_storage_rocksdb::RocksDbStorage;
use sha2::{Digest, Sha256};

const TABLE: &str = "catalogue";
/// Releases of the viewed label: fixed while the table grows.
const LABEL_RELEASES: usize = 100;
/// The live view's page.
const VIEW_ROWS: usize = 50;
const SEED_BATCH_ROWS: usize = 1_000;

/// A settled RocksDB database plus the prepared live-view query. The tempdir
/// lives as long as the database, so every sample reads the same reopened
/// persisted fixture. Built once per benchmark, outside the timed closure.
pub struct LiveViewFixture {
    _temp: tempfile::TempDir,
    db: Db,
    prepared: PreparedQuery,
    expected: Vec<RowUuid>,
}

/// What one hydration returned: rows, their digest, and logical read counts.
pub struct Hydration {
    pub row_count: usize,
    pub result_digest: String,
    pub metrics: StorageReadMetrics,
}

impl LiveViewFixture {
    pub fn new(table_rows: usize) -> Self {
        assert!(table_rows >= LABEL_RELEASES);
        let temp = tempfile::tempdir().expect("create live-view RocksDB directory");
        let seed_db = open_db(temp.path(), schema());
        seed_rows(&seed_db, table_rows);
        block_on(seed_db.close()).expect("close seeded live-view database");
        drop(seed_db);

        let db = open_db(temp.path(), schema());
        let prepared = db
            .prepare_query_bound(
                &live_view_query(),
                BTreeMap::from([("label".to_owned(), Value::Uuid(viewed_label().0))]),
            )
            .expect("prepare live-view query");
        let expected = (LABEL_RELEASES - VIEW_ROWS..LABEL_RELEASES)
            .rev()
            .map(label_release)
            .collect();
        Self {
            _temp: temp,
            db,
            prepared,
            expected,
        }
    }

    /// Untimed: the one-shot read and a maintained hydration both return the
    /// exact page and read no more than the label's indexed candidates.
    pub fn assert_selective_hydration(&self) {
        self.db.reset_storage_read_metrics_for_test();
        let rows = block_on(self.db.all_for_identity(
            &self.prepared,
            global_read_opts(),
            AuthorSubject::SYSTEM,
        ))
        .expect("run live-view query");
        let observed = rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
        assert_eq!(observed, self.expected, "live-view result changed");
        assert_selective_metrics(&self.db.take_storage_read_metrics_for_test(), "query");

        let measured = self.hydrate();
        assert_eq!(measured.row_count, self.expected.len());
        assert_eq!(measured.result_digest, digest_rows(&self.expected));
        assert_selective_metrics(&measured.metrics, "maintained");
    }

    pub fn active_groove_subscriptions(&self) -> usize {
        self.db.active_groove_subscriptions_for_test()
    }

    /// Drain the queued `SubscriptionStream::drop` finalizers and prove a
    /// repeated benchmark retains no Groove graph per sample. Outside timing.
    pub fn assert_subscription_baseline(&self, baseline: usize, phase: &str) {
        block_on(self.db.tick()).expect("drain queued live-view finalizers");
        assert_eq!(
            self.active_groove_subscriptions(),
            baseline,
            "{phase} must retire every dropped live view"
        );
    }

    /// The timed operation: open one maintained subscription and consume its
    /// initial reset. No assertion runs inside the sample.
    pub fn hydrate(&self) -> Hydration {
        self.db.reset_storage_read_metrics_for_test();
        let mut subscription =
            block_on(self.db.subscribe(&self.prepared, local_read_opts())).expect("open live view");
        let SubscriptionEvent::Delta {
            reset: true, added, ..
        } = subscription
            .try_next_event()
            .expect("live view must emit its initial reset")
        else {
            panic!("live view must emit an initial reset");
        };
        let rows = added
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        let metrics = self.db.take_storage_read_metrics_for_test();
        // Only queues teardown; the caller ticks the owner outside timing.
        drop(subscription);
        Hydration {
            row_count: rows.len(),
            result_digest: digest_rows(&rows),
            metrics,
        }
    }
}

fn live_view_query() -> Query {
    Query::from(TABLE)
        .filter(eq(col("label"), param("label")))
        .filter(eq(col("published"), lit(true)))
        .order_by("released_at", OrderDirection::Desc)
        .order_by("id", OrderDirection::Desc)
        .limit(VIEW_ROWS)
}

fn assert_selective_metrics(metrics: &StorageReadMetrics, phase: &str) {
    assert!(
        metrics.global_current_rows.reads <= LABEL_RELEASES,
        "{phase} live-view hydration read {} current rows for {LABEL_RELEASES} indexed candidates",
        metrics.global_current_rows.reads,
    );
    assert!(
        (1..=LABEL_RELEASES).contains(&metrics.global_current_indexes.reads),
        "{phase} live-view hydration must use the label index without reading more than the label's releases",
    );
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(TABLE)
                    .column("label", ColumnType::Uuid)
                    .column("published", ColumnType::Boolean)
                    .column("released_at", ColumnType::Timestamp)
                    .column("title", ColumnType::Text)
                    .index_only(["label"]),
            )
            .build(),
    )
    .expect("live-view schema compiles")
}

fn open_db(path: &Path, schema: JazzSchema) -> Db {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = RocksDbStorage::open(path, &refs).expect("open live-view RocksDB");
    block_on(Db::open_history_complete(
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
    .expect("open live-view Jazz database")
}

fn seed_rows(db: &Db, table_rows: usize) {
    for batch_start in (0..table_rows).step_by(SEED_BATCH_ROWS) {
        let batch_end = table_rows.min(batch_start + SEED_BATCH_ROWS);
        let tx = block_on(db.mergeable_tx()).expect("open live-view seed transaction");
        for index in batch_start..batch_end {
            let (row, label) = if index < LABEL_RELEASES {
                (label_release(index), viewed_label())
            } else {
                (other_release(index), other_labels())
            };
            block_on(tx.insert(
                TABLE,
                BTreeMap::from([
                    ("label".to_owned(), Value::Uuid(label.0)),
                    ("published".to_owned(), Value::Bool(true)),
                    ("released_at".to_owned(), Value::U64(index as u64)),
                    (
                        "title".to_owned(),
                        Value::String(format!("release #{index}")),
                    ),
                ]),
                jazz::db::InsertOptions {
                    row_id: Some(row),
                    ..Default::default()
                },
            ))
            .expect("stage live-view seed release");
        }
        let tx_id = block_on(tx.commit()).expect("commit live-view seed batch");
        db.finalize_local_mergeable_commit_for_test(tx_id)
            .expect("settle live-view seed batch");
    }
}

fn global_read_opts() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::Remote,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn local_read_opts() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn viewed_label() -> RowUuid {
    tagged_row(0x10, 1)
}

fn other_labels() -> RowUuid {
    tagged_row(0x20, 1)
}

fn label_release(index: usize) -> RowUuid {
    tagged_row(0x30, index as u64)
}

fn other_release(index: usize) -> RowUuid {
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

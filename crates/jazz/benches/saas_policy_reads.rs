//! SaaS-shaped read benchmark: what a client asks a central server for, when
//! row-level security is in play.
//!
//! This supersedes [`selective_global_hydration`](selective_global_hydration.rs)
//! for realistic SaaS workloads. That bench is well-formed but models a shape
//! that does not appear in a hosted multi-tenant product:
//!
//! | dimension            | `selective_global_hydration`      | this bench                                    |
//! | -------------------- | --------------------------------- | --------------------------------------------- |
//! | read policy          | none                              | `owner == @user.account OR INHERITS via org`  |
//! | identity             | `AuthorSubject::SYSTEM`           | the requesting user                            |
//! | equality bucket      | 100 rows                          | `per_owner` rows (default 10,000)              |
//! | access shape         | one equality + two orderings      | page, top-10, and org page                     |
//! | page-size sweep      | fixed at 50                       | 1 / 10 / 50 (`JAZZ_SAAS_LIMIT_LADDER`)         |
//! | scope               | one table                         | documents + org membership (a second relation) |
//!
//! Both are kept: `selective_global_hydration` remains the policy-free control
//! that isolates index selection from authorization, and this bench is the
//! realistic case. Running them side by side is the point — the policy-free arm
//! selects the declared index while the policy-scoped arm does not, and only
//! comparing them makes that visible.
//!
//! Not a receipt of a fixed target. It reports read counts and wall time so a
//! regression in boundedness is visible as a counter change rather than only as
//! latency.
//!
//! ```sh
//! cargo bench -p jazz --features testing --bench saas_policy_reads
//! ```
//!
//! Knobs: `JAZZ_SAAS_PER_OWNER` (default 10,000), `JAZZ_SAAS_TABLE_LADDER`
//! (table-size ladder), `JAZZ_SAAS_LIMIT_LADDER` (1,10,50),
//! `JAZZ_SAAS_SEED_BATCH_ROWS`, `JAZZ_SAAS_POLICY` (`owner` default, or
//! `owner_or_org`), `JAZZ_SAAS_RECEIPT=1` for the JSONL scale receipt.

mod schema_fixture;
mod support;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, LocalUpdates, MergeableTxOps, Propagation, ReadOpts,
    SeededRowIdSource, block_on,
};
use jazz::groove::db::StorageReadMetrics;
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use serde_json::{Map, json};
use support::{csv_usizes, emit_json_line, env_usize, phase_fields};

const DOCUMENTS: &str = "documents";
const ORGS: &str = "orgs";

/// Number of distinct owners the fixture spreads documents across. Each owner
/// therefore owns `table_rows / OWNERS` documents, and the equality bucket a
/// policy-scoped page has to traverse is that share.
const OWNERS: usize = 100;

/// The scale receipt prints JSONL. Divan owns stdout for the wall-time lane and
/// CodSpeed parses that stream, so receipt lines must never reach it.
fn receipt_mode() -> bool {
    std::env::var_os("JAZZ_SAAS_RECEIPT").is_some()
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();

    if receipt_mode() {
        run_receipt();
        return;
    }
    divan::main();
}

// ---------------------------------------------------------------------------
// Scale receipt
// ---------------------------------------------------------------------------

fn run_receipt() {
    let rows_ladder = csv_usizes("JAZZ_SAAS_TABLE_LADDER", "10000,100000,1000000");
    let limits = csv_usizes("JAZZ_SAAS_LIMIT_LADDER", "1,10,50");
    let batch_rows = env_usize("JAZZ_SAAS_SEED_BATCH_ROWS", 5_000);
    let policy = PolicyArm::from_env();

    for table_rows in rows_ladder {
        assert!(
            table_rows >= OWNERS,
            "table must hold at least one document per owner"
        );
        let per_owner = table_rows / OWNERS;
        let fixture = Fixture::new(table_rows, per_owner, batch_rows, policy);
        let seed_us = fixture.seed_us;

        for limit in &limits {
            fixture.emit_case("owner_page", *limit, seed_us);
        }
        fixture.emit_case("owner_recent", 10, seed_us);
        if policy == PolicyArm::OwnerOrOrg {
            fixture.emit_case("org_page", limits[0], seed_us);
        }
    }
}

// ---------------------------------------------------------------------------
// Divan wall-time benchmarks (the CodSpeed-visible lane)
// ---------------------------------------------------------------------------

#[divan::bench(sample_count = 3, sample_size = 1)]
fn saas_owner_page_10k(bencher: divan::Bencher<'_, '_>) {
    bench_case(bencher, 10_000, 50, PolicyArm::Owner);
}

#[divan::bench(sample_count = 3, sample_size = 1)]
fn saas_owner_page_100k(bencher: divan::Bencher<'_, '_>) {
    bench_case(bencher, 100_000, 50, PolicyArm::Owner);
}

#[divan::bench(sample_count = 3, sample_size = 1)]
fn saas_owner_top10_100k(bencher: divan::Bencher<'_, '_>) {
    bench_case(bencher, 100_000, 10, PolicyArm::Owner);
}

fn bench_case(bencher: divan::Bencher<'_, '_>, table_rows: usize, limit: usize, policy: PolicyArm) {
    let fixture = Fixture::new(table_rows, table_rows / OWNERS, 5_000, policy);
    bencher.bench_local(|| divan::black_box(fixture.owner_page(limit)));
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PolicyArm {
    /// `owner == @user.account`. The common SaaS shape.
    Owner,
    /// `owner == @user.account OR INHERITS via org`, with org membership. The
    /// alternative-authorization shape whose full-scan fallback is documented in
    /// `read_sources.rs`.
    OwnerOrOrg,
}

impl PolicyArm {
    fn from_env() -> Self {
        match std::env::var("JAZZ_SAAS_POLICY").as_deref() {
            Ok("owner_or_org") => Self::OwnerOrOrg,
            _ => Self::Owner,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::OwnerOrOrg => "owner_or_org",
        }
    }
}

fn user(index: usize) -> AuthorSubject {
    // A UUID-backed test subject that also carries the matching admitted
    // account, so `@user.account` resolves to a Uuid the fixture can store.
    let mut bytes = [0u8; 16];
    bytes[0] = 0x40;
    bytes[6] = 0x40;
    bytes[8] = 0x80;
    bytes[9..].copy_from_slice(&(index as u64).to_be_bytes()[1..]);
    schema_fixture::account_author_uuid(uuid::Uuid::from_bytes(bytes))
}

fn tagged(tag: u8, index: u64) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0] = tag;
    b[8..].copy_from_slice(&index.to_be_bytes());
    b
}

fn document_row(index: usize) -> RowUuid {
    RowUuid::from_bytes(tagged(0x92, index as u64))
}

fn org_row(index: usize) -> RowUuid {
    RowUuid::from_bytes(tagged(0x90, index as u64))
}

fn schema(policy: PolicyArm) -> JazzSchema {
    let mut orgs = TableSchemaBuilder::new(ORGS)
        .column("member_id", ColumnType::Uuid)
        .column("name", ColumnType::Text);
    if policy == PolicyArm::OwnerOrOrg {
        // The inherited hop reads this table's SELECT policy, so membership must
        // be a real predicate on a real column.
        orgs = orgs.policies(schema_fixture::select_only(
            schema_fixture::session_user_id_column("member_id"),
        ));
    }

    // `index_only` declares independent single-column global-current indexes.
    // Listing `updated_at` does not create an ordered `(owner_id, updated_at)`
    // access path; `global_current_indexed_columns()` is a set and each entry
    // becomes its own index. The bench therefore measures the planner that
    // exists rather than assuming a compound path.
    let mut documents = TableSchemaBuilder::new(DOCUMENTS)
        .column("owner_id", ColumnType::Uuid)
        .column("done", ColumnType::Boolean)
        .column("updated_at", ColumnType::Timestamp)
        .column("title", ColumnType::Text)
        .index_only(["owner_id", "updated_at"]);
    documents = match policy {
        PolicyArm::Owner => documents.policies(schema_fixture::select_only(
            schema_fixture::session_user_id_column("owner_id"),
        )),
        PolicyArm::OwnerOrOrg => {
            documents
                .fk_column("org_id", ORGS)
                .policies(schema_fixture::select_only(
                    schema_fixture::owner_or_org_access("owner_id", "org_id"),
                ))
        }
    };
    schema_fixture::compile(SchemaBuilder::new().table(orgs).table(documents))
}

fn open_db(path: &Path, schema: &JazzSchema) -> Db<RocksDbStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open_history_complete(
        DbConfig::new(
            schema.clone(),
            RocksDbStorage::open_with_durability(path, &refs, Durability::WalNoSync)
                .expect("open saas RocksDB"),
            DbIdentity {
                node: NodeUuid::from_bytes([0x4b; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x4b)),
    ))
    .expect("open saas Jazz database")
}

struct Fixture {
    seed_us: u128,
    table_rows: usize,
    per_owner: usize,
    policy: PolicyArm,
    query_owner: usize,
    schema: JazzSchema,
    path: std::path::PathBuf,
    _temp: tempfile::TempDir,
}

impl Fixture {
    fn new(table_rows: usize, per_owner: usize, batch_rows: usize, policy: PolicyArm) -> Self {
        assert!(per_owner > 0, "per-owner share must be positive");
        assert!(
            table_rows >= OWNERS,
            "table must hold at least one document per owner"
        );
        let temp = tempfile::tempdir().expect("saas RocksDB directory");
        let schema = schema(policy);
        let db = open_db(temp.path(), &schema);

        let seed_started = Instant::now();
        seed_rows(&db, table_rows, per_owner, batch_rows, policy);
        let seed_us = seed_started.elapsed().as_micros();
        block_on(db.close()).expect("close seeded saas database");
        drop(db);

        let _db = open_db(temp.path(), &schema);
        if receipt_mode() {
            let mut fields = phase_fields("saas_seed", seed_us);
            fields.insert("table_rows".to_owned(), json!(table_rows));
            fields.insert("per_owner".to_owned(), json!(per_owner));
            fields.insert("policy".to_owned(), json!(policy.label()));
            fields.insert("seed_us".to_owned(), json!(seed_us));
            emit_json_line("saas_policy_reads", fields);
        }

        Self {
            seed_us,
            table_rows,
            per_owner,
            policy,
            query_owner: 3,
            schema,
            path: temp.path().to_path_buf(),
            _temp: temp,
        }
    }

    /// Read one owner page and report the counters that decide whether the read
    /// was bounded by the page or by the owner's whole scope.
    fn owner_page(&self, limit: usize) -> CaseMeasurement {
        let owner = user(self.query_owner);
        let query = Query::from(DOCUMENTS)
            .filter(eq(
                col("owner_id"),
                jazz::query::lit(Value::Uuid(owner.test_uuid())),
            ))
            .order_by("updated_at", OrderDirection::Desc)
            .limit(limit);
        self.run("owner_page", query, limit, owner)
    }

    fn owner_recent(&self, limit: usize) -> CaseMeasurement {
        let owner = user(self.query_owner);
        let query = Query::from(DOCUMENTS)
            .filter(eq(
                col("owner_id"),
                jazz::query::lit(Value::Uuid(owner.test_uuid())),
            ))
            .order_by("updated_at", OrderDirection::Desc)
            .limit(limit);
        self.run("owner_recent", query, limit, owner)
    }

    fn org_page(&self, limit: usize) -> CaseMeasurement {
        let owner = user(self.query_owner);
        let org = org_row(self.query_owner / self.per_owner.max(1));
        let query = Query::from(DOCUMENTS)
            .filter(eq(col("org_id"), jazz::query::lit(Value::Uuid(org.0))))
            .order_by("updated_at", OrderDirection::Desc)
            .limit(limit);
        self.run("org_page", query, limit, owner)
    }

    fn run(
        &self,
        case: &'static str,
        query: Query,
        limit: usize,
        identity: AuthorSubject,
    ) -> CaseMeasurement {
        // Reopen per case. A second query on the same instance is served from
        // the retained maintained view and reports zero reads; that measures the
        // cache, not planning. Reopening keeps every case a cold read while
        // leaving the persisted fixture identical.
        let db = open_db(&self.path, &self.schema);
        let prepared = db
            .prepare_query_bound(&query, BTreeMap::new())
            .expect("prepare saas query");
        db.reset_storage_read_metrics_for_test();
        let started = Instant::now();
        let rows = block_on(db.all_for_identity(&prepared, global_read_opts(), identity))
            .expect("run saas query");
        let us = started.elapsed().as_micros();
        let metrics = db.take_storage_read_metrics_for_test();
        block_on(db.close()).expect("close case database");
        CaseMeasurement {
            case,
            table_rows: self.table_rows,
            per_owner: self.per_owner,
            policy: self.policy,
            limit,
            result_rows: rows.len(),
            us,
            metrics,
        }
    }

    fn emit_case(&self, case: &str, limit: usize, _seed_us: u128) {
        let measurement = match case {
            "owner_page" => self.owner_page(limit),
            "owner_recent" => self.owner_recent(limit),
            "org_page" => self.org_page(limit),
            other => panic!("unknown saas case {other}"),
        };
        if receipt_mode() {
            measurement.emit();
        }
    }
}

struct CaseMeasurement {
    case: &'static str,
    table_rows: usize,
    per_owner: usize,
    policy: PolicyArm,
    limit: usize,
    result_rows: usize,
    us: u128,
    metrics: StorageReadMetrics,
}

impl CaseMeasurement {
    fn emit(&self) {
        let mut fields = phase_fields(self.case, self.us);
        fields.insert("table_rows".to_owned(), json!(self.table_rows));
        fields.insert("per_owner".to_owned(), json!(self.per_owner));
        fields.insert("policy".to_owned(), json!(self.policy.label()));
        fields.insert("limit".to_owned(), json!(self.limit));
        fields.insert("result_rows".to_owned(), json!(self.result_rows));
        fields.insert("query_us".to_owned(), json!(self.us));
        insert_read_fields(&mut fields, &self.metrics);
        // Read amplification against the requested page. A bounded read is O(1)
        // here; a full-scope traversal grows with `per_owner`.
        let logical = self.metrics.total.reads;
        fields.insert(
            "reads_per_result_row".to_owned(),
            json!(ratio(logical, self.result_rows)),
        );
        emit_json_line("saas_policy_reads", fields);
    }
}

fn insert_read_fields(fields: &mut Map<String, serde_json::Value>, metrics: &StorageReadMetrics) {
    fields.insert(
        "index_reads".to_owned(),
        json!(metrics.global_current_indexes.reads),
    );
    fields.insert(
        "current_row_reads".to_owned(),
        json!(metrics.global_current_rows.reads),
    );
    fields.insert(
        "history_row_reads".to_owned(),
        json!(metrics.history_rows.reads),
    );
    fields.insert("total_reads".to_owned(), json!(metrics.total.reads));
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    (denominator != 0).then(|| numerator as f64 / denominator as f64)
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

fn seed_rows(
    db: &Db<RocksDbStorage>,
    table_rows: usize,
    per_owner: usize,
    batch_rows: usize,
    policy: PolicyArm,
) {
    if policy == PolicyArm::OwnerOrOrg {
        let orgs = table_rows.div_ceil(per_owner).max(1) + 1;
        let tx = block_on(db.mergeable_tx()).expect("open saas org seed tx");
        for org in 0..orgs {
            block_on(tx.insert(
                ORGS,
                BTreeMap::from([
                    ("member_id".to_owned(), Value::Uuid(user(3).test_uuid())),
                    ("name".to_owned(), Value::String(format!("org-{org}"))),
                ]),
                InsertOptions {
                    row_id: Some(org_row(org)),
                    ..Default::default()
                },
            ))
            .expect("stage saas org row");
        }
        let tx_id = block_on(tx.commit()).expect("commit saas org seed tx");
        db.finalize_local_mergeable_commit_for_test(tx_id)
            .expect("settle saas org seed tx");
    }

    for batch_start in (0..table_rows).step_by(batch_rows) {
        let batch_end = table_rows.min(batch_start + batch_rows);
        let tx = block_on(db.mergeable_tx()).expect("open saas seed tx");
        for index in batch_start..batch_end {
            let owner = index / per_owner;
            let mut cells = BTreeMap::from([
                ("owner_id".to_owned(), Value::Uuid(user(owner).test_uuid())),
                ("done".to_owned(), Value::Bool(index % 3 == 0)),
                ("updated_at".to_owned(), Value::U64(index as u64)),
                (
                    "title".to_owned(),
                    Value::String(format!("document-{index}")),
                ),
            ]);
            if policy == PolicyArm::OwnerOrOrg {
                cells.insert(
                    "org_id".to_owned(),
                    Value::Uuid(org_row(owner / per_owner.max(1)).0),
                );
            }
            block_on(tx.insert(
                DOCUMENTS,
                cells,
                InsertOptions {
                    row_id: Some(document_row(index)),
                    ..Default::default()
                },
            ))
            .expect("stage saas document");
        }
        let tx_id = block_on(tx.commit()).expect("commit saas seed tx");
        db.finalize_local_mergeable_commit_for_test(tx_id)
            .expect("settle saas seed tx");
    }
}

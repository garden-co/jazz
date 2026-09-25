//! Public-API document sharing fixture, adapted from Tobias Lins's #2996.
//! Query-only timing with a separate reopen/first-read receipt. See the example README.
use std::{collections::BTreeMap, path::Path, time::Instant};

use jazz::db::{
    Db, DbConfig, DbIdentity, DbOpenReceipt, InsertOptions, LocalUpdates, MergeableTxOps,
    PreparedQuery, Propagation, ReadOpts, SeededRowIdSource, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::groove::{db::StorageReadMetrics, records::Value};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::CurrentRow;
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{Operation, PolicyExpr};
use jazz::tools::{ColumnType, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_storage_rocksdb::{Durability, RocksDbStorage};

pub const OWNERS: usize = 100;
pub const OWNERS_PER_ORG: usize = 4;
// Owner 3 has newer rows, so the member's organization page exercises inherited
// access, not just the direct-owner arm of the OR policy.
pub const QUERY_OWNER: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    Unrestricted,
    Owner,
    OwnerOrOrg,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Page {
    Owner(usize),
    Org(usize),
}

pub fn user(index: usize) -> AuthorSubject {
    let mut bytes = tagged(0x40, index);
    bytes[6] = 0x40;
    bytes[8] = 0x80;
    let id = uuid::Uuid::from_bytes(bytes);
    AuthorSubject::for_test_uuid(id).with_account(jazz::account_registry::AccountId(id))
}

fn tagged(tag: u8, index: usize) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[0] = tag;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    bytes
}

pub fn document_row(index: usize) -> RowUuid {
    RowUuid::from_bytes(tagged(0x92, index))
}

fn org_row(index: usize) -> RowUuid {
    RowUuid::from_bytes(tagged(0x90, index))
}

pub fn schema(policy: Policy) -> JazzSchema {
    schema_with_composite_indexes(policy, true)
}

pub fn schema_with_composite_indexes(policy: Policy, composite_indexes: bool) -> JazzSchema {
    let account =
        |column: &str| PolicyExpr::eq_session(column, vec!["user".into(), "account".into()]);
    let orgs = TableSchemaBuilder::new("orgs")
        .column("member_id", ColumnType::Uuid)
        .column("name", ColumnType::Text)
        .policies(TablePolicies::new().with_select(account("member_id")));
    let mut documents = TableSchemaBuilder::new("documents")
        .column("owner_id", ColumnType::Uuid)
        .column("done", ColumnType::Boolean)
        .column("updated_at", ColumnType::Timestamp)
        .column("title", ColumnType::Text)
        .fk_column("org_id", "orgs")
        .index_only(["owner_id", "org_id", "updated_at"]);
    if composite_indexes {
        documents = documents
            .composite_index(["owner_id", "updated_at"])
            .composite_index(["org_id", "updated_at"]);
    }
    documents = match policy {
        // Missing policies deny reads; this control explicitly permits all
        // document rows while retaining the same non-SYSTEM identity.
        Policy::Unrestricted => {
            documents.policies(TablePolicies::new().with_select(PolicyExpr::True))
        }
        Policy::Owner => documents.policies(TablePolicies::new().with_select(account("owner_id"))),
        Policy::OwnerOrOrg => {
            documents.policies(TablePolicies::new().with_select(PolicyExpr::or(vec![
                account("owner_id"),
                PolicyExpr::Inherits {
                    operation: Operation::Select,
                    via_column: "org_id".into(),
                    max_depth: None,
                },
            ])))
        }
    };
    JazzSchema::new(&SchemaBuilder::new().table(orgs).table(documents).build())
        .expect("compile public document schema")
}

fn open(path: &Path, schema: &JazzSchema) -> Db<RocksDbStorage> {
    open_measured(path, schema).0
}

struct OpenTimings {
    storage_us: u128,
    jazz_us: u128,
    receipt: DbOpenReceipt,
}

fn open_measured(path: &Path, schema: &JazzSchema) -> (Db<RocksDbStorage>, OpenTimings) {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let start = Instant::now();
    let storage = RocksDbStorage::open_with_durability(path, &refs, Durability::WalNoSync)
        .expect("open document RocksDB");
    let storage_us = start.elapsed().as_micros();
    let start = Instant::now();
    let (db, receipt) = block_on(Db::open_history_complete_with_receipt_for_test(
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x4b; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x4b)),
    ))
    .expect("open document database");
    let jazz_us = start.elapsed().as_micros();
    (
        db,
        OpenTimings {
            storage_us,
            jazz_us,
            receipt,
        },
    )
}

pub struct Fixture {
    pub seed_us: u128,
    pub table_rows: usize,
    pub policy: Policy,
    schema: JazzSchema,
    directory: tempfile::TempDir,
}

impl Fixture {
    pub fn new(table_rows: usize, policy: Policy) -> Self {
        Self::with_order_values_and_indexes(table_rows, policy, true, |index| index as u64)
    }

    pub fn with_composite_indexes(
        table_rows: usize,
        policy: Policy,
        composite_indexes: bool,
    ) -> Self {
        Self::with_order_values_and_indexes(table_rows, policy, composite_indexes, |index| {
            index as u64
        })
    }

    pub fn with_order_values(
        table_rows: usize,
        policy: Policy,
        order_value: impl Fn(usize) -> u64,
    ) -> Self {
        Self::with_order_values_and_indexes(table_rows, policy, true, order_value)
    }

    pub fn with_order_values_and_indexes(
        table_rows: usize,
        policy: Policy,
        composite_indexes: bool,
        order_value: impl Fn(usize) -> u64,
    ) -> Self {
        assert!(table_rows >= OWNERS && table_rows.is_multiple_of(OWNERS));
        let directory = tempfile::tempdir().expect("fixture directory");
        let schema = schema_with_composite_indexes(policy, composite_indexes);
        let db = open(directory.path(), &schema);
        let start = Instant::now();
        let tx = block_on(db.mergeable_tx()).expect("org seed tx");
        for org in 0..OWNERS / OWNERS_PER_ORG {
            block_on(tx.insert(
                "orgs",
                BTreeMap::from([
                    (
                        "member_id".into(),
                        Value::Uuid(user(org * OWNERS_PER_ORG + QUERY_OWNER).test_uuid()),
                    ),
                    ("name".into(), Value::String(format!("org-{org}"))),
                ]),
                InsertOptions {
                    row_id: Some(org_row(org)),
                    ..Default::default()
                },
            ))
            .expect("seed org");
        }
        let id = block_on(tx.commit()).expect("commit orgs");
        db.finalize_local_mergeable_commit_for_test(id)
            .expect("settle orgs");
        for start in (0..table_rows).step_by(5_000) {
            let tx = block_on(db.mergeable_tx()).expect("document seed tx");
            for index in start..table_rows.min(start + 5_000) {
                let owner = index / (table_rows / OWNERS);
                block_on(tx.insert(
                    "documents",
                    BTreeMap::from([
                        ("owner_id".into(), Value::Uuid(user(owner).test_uuid())),
                        ("done".into(), Value::Bool(index % 3 == 0)),
                        ("updated_at".into(), Value::U64(order_value(index))),
                        ("title".into(), Value::String(format!("document-{index}"))),
                        (
                            "org_id".into(),
                            Value::Uuid(org_row(owner / OWNERS_PER_ORG).0),
                        ),
                    ]),
                    InsertOptions {
                        row_id: Some(document_row(index)),
                        ..Default::default()
                    },
                ))
                .expect("seed document");
            }
            let id = block_on(tx.commit()).expect("commit documents");
            db.finalize_local_mergeable_commit_for_test(id)
                .expect("settle documents");
        }
        let seed_us = start.elapsed().as_micros();
        block_on(db.close()).expect("close seed database");
        Self {
            seed_us,
            table_rows,
            policy,
            schema,
            directory,
        }
    }

    pub fn delete_documents(&self, indices: &[usize]) {
        let db = open(self.directory.path(), &self.schema);
        let tx = block_on(db.mergeable_tx()).expect("document deletion tx");
        for &index in indices {
            block_on(tx.delete("documents", document_row(index), Default::default()))
                .expect("delete document");
        }
        let id = block_on(tx.commit()).expect("commit document deletions");
        db.finalize_local_mergeable_commit_for_test(id)
            .expect("settle document deletions");
        block_on(db.close()).expect("close deletion database");
    }

    pub fn restore_documents(&self, indices: &[usize], order_value: impl Fn(usize) -> u64) {
        let db = open(self.directory.path(), &self.schema);
        let tx = block_on(db.mergeable_tx()).expect("document restoration tx");
        for &index in indices {
            let owner = index / (self.table_rows / OWNERS);
            block_on(tx.restore(
                "documents",
                document_row(index),
                Some(BTreeMap::from([
                    ("owner_id".into(), Value::Uuid(user(owner).test_uuid())),
                    ("done".into(), Value::Bool(index % 3 == 0)),
                    ("updated_at".into(), Value::U64(order_value(index))),
                    ("title".into(), Value::String(format!("document-{index}"))),
                    (
                        "org_id".into(),
                        Value::Uuid(org_row(owner / OWNERS_PER_ORG).0),
                    ),
                ])),
                Default::default(),
            ))
            .expect("restore document");
        }
        let id = block_on(tx.commit()).expect("commit document restorations");
        db.finalize_local_mergeable_commit_for_test(id)
            .expect("settle document restorations");
        block_on(db.close()).expect("close restoration database");
    }

    /// Runtime-cold, not OS-cache-cold. Only one session may be live per fixture.
    pub fn session(&self, page: Page, limit: usize, identity: AuthorSubject) -> Session {
        let start = Instant::now();
        let (db, open_timings) = open_measured(self.directory.path(), &self.schema);
        let reopen_us = start.elapsed().as_micros();
        let start = Instant::now();
        let predicate = match page {
            Page::Owner(owner) => eq(col("owner_id"), lit(Value::Uuid(user(owner).test_uuid()))),
            Page::Org(org) => eq(col("org_id"), lit(Value::Uuid(org_row(org).0))),
        };
        let query = Query::from("documents")
            .filter(predicate)
            .order_by("updated_at", OrderDirection::Desc)
            .limit(limit);
        let prepared = db
            .prepare_query_bound(&query, BTreeMap::new())
            .expect("prepare page");
        let prepare_us = start.elapsed().as_micros();
        db.reset_storage_read_metrics_for_test();
        Session {
            db,
            prepared,
            identity,
            reopen_us,
            storage_open_us: open_timings.storage_us,
            jazz_open_us: open_timings.jazz_us,
            open_receipt: open_timings.receipt,
            prepare_us,
            executed: false,
        }
    }
}

pub struct Session {
    db: Db<RocksDbStorage>,
    prepared: PreparedQuery,
    identity: AuthorSubject,
    pub reopen_us: u128,
    pub storage_open_us: u128,
    pub jazz_open_us: u128,
    pub open_receipt: DbOpenReceipt,
    pub prepare_us: u128,
    executed: bool,
}

impl Session {
    /// Paired first-result endpoint. The stream stays alive in the returned
    /// value so Divan excludes its finalization, as it excludes read teardown.
    pub fn subscribe(&mut self) -> (SubscriptionStream, SubscriptionEvent) {
        assert!(!self.executed, "a cold sample must use a fresh runtime");
        self.executed = true;
        let mut stream = block_on(self.db.subscribe_for_identity(
            &self.prepared,
            ReadOpts {
                tier: DurabilityTier::Local,
                local_updates: LocalUpdates::Immediate,
                propagation: Propagation::LocalOnly,
                include_deleted: false,
                ..ReadOpts::default()
            },
            self.identity,
        ))
        .expect("subscribe document page");
        // The standalone native harness owns the runtime loop (there is no
        // JS/server host driving cold subscription progress in the background).
        let hydration_start = Instant::now();
        let initial = loop {
            if let Some(event) = stream.try_next_event() {
                break event;
            }
            block_on(self.db.tick()).expect("drive subscription hydration");
            if hydration_start.elapsed().as_secs() > 600 {
                panic!(
                    "subscription did not hydrate: {}",
                    self.db.query_delivery_diagnostics_for_test()
                );
            }
        };
        (stream, initial)
    }

    pub fn read(&mut self) -> Vec<CurrentRow> {
        assert!(
            !self.executed,
            "a cold-query sample must use a fresh runtime"
        );
        self.executed = true;
        block_on(self.db.all_for_identity(
            &self.prepared,
            ReadOpts {
                tier: DurabilityTier::Global,
                local_updates: LocalUpdates::Deferred,
                propagation: Propagation::LocalOnly,
                include_deleted: false,
                ..ReadOpts::default()
            },
            self.identity,
        ))
        .expect("read document page")
    }

    pub fn take_metrics(&self) -> StorageReadMetrics {
        self.db.take_storage_read_metrics_for_test()
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        block_on(self.db.close()).expect("close document session");
    }
}

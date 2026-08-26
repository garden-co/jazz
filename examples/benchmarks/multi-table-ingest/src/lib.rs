//! Neutral synthetic multi-table ingest fixture for thesis #2030.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, Transport, WriteIdentity, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::{MemoryStorage, OrderedKvStorage, ReopenableStorage};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::SyncMessage;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, Schema, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;
use jazz::wire::TransportError;
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use tempfile::TempDir;

const GROUPS: usize = 32;
const RESOURCES: usize = 1_000;

mod public_client;
pub use public_client::ClientIngestFixture;

pub struct IngestFixture<S: OrderedKvStorage> {
    db: Db<S>,
    next_job: usize,
    write_identity: WriteIdentity,
    check_write_state: bool,
    tick_after_write: bool,
}

impl IngestFixture<MemoryStorage> {
    pub fn memory(existing_jobs: usize) -> Self {
        Self::memory_with_attribution(existing_jobs, false)
    }

    pub fn memory_attributed(existing_jobs: usize) -> Self {
        Self::memory_with_attribution(existing_jobs, true)
    }

    pub fn memory_attributed_with_exists_policy(existing_jobs: usize) -> Self {
        let schema = schema(true);
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        Self::new(
            schema,
            MemoryStorage::new(&refs),
            existing_jobs,
            true,
            false,
            false,
        )
    }

    pub fn memory_with_write_state_check(existing_jobs: usize) -> Self {
        let schema = schema(false);
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        Self::new(
            schema,
            MemoryStorage::new(&refs),
            existing_jobs,
            false,
            true,
            false,
        )
    }

    pub fn memory_with_stalled_upstream(existing_jobs: usize) -> Self {
        let mut fixture = Self::memory_with_attribution(existing_jobs, false);
        let _connection = block_on(fixture.db.connect_upstream(Box::new(StalledTransport)));
        fixture.tick_after_write = true;
        fixture
    }

    fn memory_with_attribution(existing_jobs: usize, attributed: bool) -> Self {
        let schema = schema(false);
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        Self::new(
            schema,
            MemoryStorage::new(&refs),
            existing_jobs,
            attributed,
            false,
            false,
        )
    }
}

struct StalledTransport;

impl Transport for StalledTransport {
    fn send(&mut self, _message: SyncMessage) -> Result<(), TransportError> {
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
}

impl IngestFixture<RocksDbStorage> {
    pub fn rocksdb(existing_jobs: usize) -> (TempDir, Self) {
        let schema = schema(false);
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let dir = tempfile::tempdir().expect("create ingest benchmark directory");
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync)
                .expect("open ingest benchmark RocksDB");
        (
            dir,
            Self::new(schema, storage, existing_jobs, false, false, false),
        )
    }

    pub fn rocksdb_with_tick(existing_jobs: usize) -> (TempDir, Self) {
        let schema = schema(false);
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let dir = tempfile::tempdir().expect("create ingest benchmark directory");
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync)
                .expect("open ingest benchmark RocksDB");
        (
            dir,
            Self::new(schema, storage, existing_jobs, false, false, true),
        )
    }
}

impl<S: OrderedKvStorage + ReopenableStorage + 'static> IngestFixture<S> {
    fn new(
        schema: JazzSchema,
        storage: S,
        existing_jobs: usize,
        attributed: bool,
        check_write_state: bool,
        tick_after_write: bool,
    ) -> Self {
        let config = DbConfig::new(
            schema,
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x83; 16]),
                author: AuthorSubject::SYSTEM,
            },
        );
        let db = if attributed {
            // SAFETY: this synthetic process owns the isolated benchmark DB and
            // never exposes its trusted-backend capability to another caller.
            unsafe { block_on(Db::open_with_backend_attribution(config)) }
        } else {
            block_on(Db::open(config))
        }
        .expect("open multi-table ingest database");
        let write_identity = if attributed {
            WriteIdentity::Attribution(
                AuthorSubject::authenticated("https://benchmark.invalid", "synthetic-writer")
                    .expect("synthetic attributed identity"),
            )
        } else {
            WriteIdentity::Database
        };
        let mut fixture = Self {
            db,
            next_job: 0,
            write_identity,
            check_write_state,
            tick_after_write,
        };
        fixture.seed_dimensions();
        fixture.insert_jobs(existing_jobs);
        fixture
    }

    pub fn insert_next_1k(mut self) -> usize {
        self.insert_jobs(1_000);
        self.next_job
    }

    fn seed_dimensions(&mut self) {
        for group in 0..GROUPS {
            self.insert(
                "groups",
                row_id(1, group),
                BTreeMap::from([
                    ("tenant".to_owned(), tenant(group)),
                    (
                        "label".to_owned(),
                        Value::String(format!("Group {group:03}")),
                    ),
                ]),
            );
        }
        for resource in 0..RESOURCES {
            let mut cells = BTreeMap::from([
                ("tenant".to_owned(), tenant(resource)),
                (
                    "label".to_owned(),
                    Value::String(format!("Resource {resource:05}")),
                ),
                (
                    "summary".to_owned(),
                    Value::String(format!("Synthetic resource {resource:05}")),
                ),
                (
                    "status".to_owned(),
                    Value::String(["READY", "PAUSED", "ERROR", "ACTIVE"][resource % 4].into()),
                ),
                (
                    "phase".to_owned(),
                    Value::String(["IDLE", "HELD", "ASSIGNED", "WORKING"][resource % 4].into()),
                ),
                (
                    "group_id".to_owned(),
                    nullable(Value::Uuid(row_id(1, resource % GROUPS).0)),
                ),
                (
                    "heartbeat_at".to_owned(),
                    nullable(Value::U64(resource as u64)),
                ),
                (
                    "version".to_owned(),
                    Value::String(format!("v{}.{}", resource % 4, resource % 10)),
                ),
                (
                    "latitude".to_owned(),
                    nullable(Value::F64((resource % 90) as f64)),
                ),
                (
                    "longitude".to_owned(),
                    nullable(Value::F64((resource % 180) as f64)),
                ),
                (
                    "capacity".to_owned(),
                    nullable(Value::F64((resource % 100) as f64)),
                ),
                ("uptime".to_owned(), nullable(Value::I32(resource as i32))),
            ]);
            if resource % 3 == 0 {
                cells.insert(
                    "held_by".to_owned(),
                    nullable(Value::String(format!("user-{}", resource % 20))),
                );
                cells.insert("held_at".to_owned(), nullable(Value::U64(resource as u64)));
            }
            self.insert("resources", row_id(2, resource), cells);
        }
    }

    fn insert_jobs(&mut self, count: usize) {
        let start = self.next_job;
        for job in start..start + count {
            let mut cells = BTreeMap::from([
                ("tenant".to_owned(), tenant(job)),
                (
                    "group_id".to_owned(),
                    Value::Uuid(row_id(1, job % GROUPS).0),
                ),
                (
                    "status".to_owned(),
                    Value::String(["OPEN", "ASSIGNED", "DONE", "VERIFIED"][job % 4].into()),
                ),
                ("title".to_owned(), Value::String(format!("Job {job:07}"))),
                ("reward".to_owned(), Value::I32((job % 10_000) as i32)),
                (
                    "external_key".to_owned(),
                    Value::String(format!("key-{job:07}")),
                ),
            ]);
            if job % 4 == 0 {
                cells.insert(
                    "resource_id".to_owned(),
                    nullable(Value::Uuid(row_id(2, job % RESOURCES).0)),
                );
            }
            self.insert("jobs", row_id(3, job), cells);
        }
        self.next_job += count;
    }

    fn insert(&self, table: &str, row_id: RowUuid, cells: BTreeMap<String, Value>) {
        let write = block_on(self.db.insert(
            table,
            cells,
            InsertOptions {
                row_id: Some(row_id),
                identity: self.write_identity,
                ..Default::default()
            },
        ))
        .unwrap_or_else(|error| panic!("insert synthetic {table} row: {error}"));
        if self.check_write_state {
            self.db
                .write_state(write.mergeable_tx_id())
                .unwrap_or_else(|error| panic!("inspect synthetic {table} write fate: {error}"));
        }
        block_on(write.wait(DurabilityTier::Local))
            .unwrap_or_else(|error| panic!("wait for synthetic {table} row: {error}"));
        if self.tick_after_write {
            block_on(self.db.tick())
                .unwrap_or_else(|error| panic!("tick after synthetic {table} row: {error}"));
        }
    }
}

fn schema(with_exists_policy: bool) -> JazzSchema {
    JazzSchema::new(&public_schema(with_exists_policy)).expect("multi-table ingest schema compiles")
}

fn public_schema(with_exists_policy: bool) -> Schema {
    let jobs_policies = if with_exists_policy {
        TablePolicies::new().with_insert(PolicyExpr::Exists {
            table: "groups".to_owned(),
            condition: Box::new(PolicyExpr::Cmp {
                column: "id".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec![
                    "__jazz_outer_row".to_owned(),
                    "group_id".to_owned(),
                ]),
            }),
        })
    } else {
        TablePolicies::new()
    };
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("groups")
                .column("tenant", ColumnType::Text)
                .column("label", ColumnType::Text)
                .index_only(["tenant"]),
        )
        .table(
            TableSchemaBuilder::new("resources")
                .column("tenant", ColumnType::Text)
                .column("label", ColumnType::Text)
                .column("summary", ColumnType::Text)
                .column(
                    "status",
                    string_enum(["READY", "PAUSED", "ERROR", "ACTIVE"]),
                )
                .column(
                    "phase",
                    string_enum(["IDLE", "HELD", "ASSIGNED", "WORKING"]),
                )
                .nullable_fk_column("group_id", "groups")
                .nullable_column("held_by", ColumnType::Text)
                .nullable_column("held_at", ColumnType::Timestamp)
                .nullable_column("heartbeat_at", ColumnType::Timestamp)
                .column("version", ColumnType::Text)
                .nullable_column("latitude", ColumnType::Double)
                .nullable_column("longitude", ColumnType::Double)
                .nullable_column("capacity", ColumnType::Double)
                .nullable_column("uptime", ColumnType::Integer)
                .index_only(["tenant", "group_id", "phase", "held_by"]),
        )
        .table(
            TableSchemaBuilder::new("jobs")
                .column("tenant", ColumnType::Text)
                .fk_column("group_id", "groups")
                .nullable_fk_column("resource_id", "resources")
                .column(
                    "status",
                    string_enum(["OPEN", "ASSIGNED", "DONE", "VERIFIED"]),
                )
                .column("title", ColumnType::Text)
                .column("reward", ColumnType::Integer)
                .column("external_key", ColumnType::Text)
                .index_only(["tenant", "group_id", "status", "resource_id"])
                .policies(jobs_policies),
        )
        .table(
            TableSchemaBuilder::new("sessions")
                .fk_column("resource_id", "resources")
                .column("user", ColumnType::Text)
                .column("started_at", ColumnType::Timestamp),
        )
        .build()
}

fn string_enum<const N: usize>(variants: [&str; N]) -> ColumnType {
    ColumnType::Enum {
        variants: variants.into_iter().map(str::to_owned).collect(),
    }
}

fn tenant(index: usize) -> Value {
    Value::String(format!("tenant-{}", index % 2))
}

fn nullable(value: Value) -> Value {
    Value::Nullable(Some(Box::new(value)))
}

fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

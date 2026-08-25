use std::collections::HashMap;
use std::sync::Arc;

use jazz::db::block_on;
use jazz::tools::{AppContext, AppId, ClientStorage, JazzClient, ObjectId, Value};
use tempfile::TempDir;

use crate::{GROUPS, RESOURCES, public_schema, row_id};

pub struct ClientIngestFixture {
    _dir: TempDir,
    client: JazzClient,
    next_job: usize,
}

impl ClientIngestFixture {
    pub fn memory(existing_jobs: usize) -> Self {
        Self::new(existing_jobs, ClientStorage::Memory)
    }

    pub fn persistent(existing_jobs: usize) -> Self {
        Self::new(existing_jobs, ClientStorage::Persistent)
    }

    fn new(existing_jobs: usize, storage: ClientStorage) -> Self {
        let dir = tempfile::tempdir().expect("create public-client benchmark directory");
        let storage_factory = matches!(storage, ClientStorage::Persistent)
            .then(|| Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory) as Arc<_>);
        let client = block_on(JazzClient::connect(AppContext {
            app_id: AppId::from_name("neutral-multi-table-ingest"),
            client_id: None,
            schema: public_schema(false),
            server_url: String::new(),
            data_dir: dir.path().to_path_buf(),
            storage,
            storage_factory,
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
        }))
        .expect("open public JazzClient benchmark database");
        let mut fixture = Self {
            _dir: dir,
            client,
            next_job: 0,
        };
        fixture.seed_dimensions();
        fixture.insert_jobs(existing_jobs);
        fixture
    }

    pub fn insert_next_1k(mut self) -> usize {
        self.insert_jobs(1_000);
        self.next_job
    }

    fn seed_dimensions(&self) {
        for group in 0..GROUPS {
            self.insert(
                "groups",
                1,
                group,
                HashMap::from([
                    ("tenant".to_owned(), tenant(group)),
                    ("label".to_owned(), Value::Text(format!("Group {group:03}"))),
                ]),
            );
        }
        for resource in 0..RESOURCES {
            self.insert(
                "resources",
                2,
                resource,
                HashMap::from([
                    ("tenant".to_owned(), tenant(resource)),
                    (
                        "label".to_owned(),
                        Value::Text(format!("Resource {resource:05}")),
                    ),
                    (
                        "summary".to_owned(),
                        Value::Text(format!("Synthetic resource {resource:05}")),
                    ),
                    (
                        "status".to_owned(),
                        Value::Text(["READY", "PAUSED", "ERROR", "ACTIVE"][resource % 4].into()),
                    ),
                    (
                        "phase".to_owned(),
                        Value::Text(["IDLE", "HELD", "ASSIGNED", "WORKING"][resource % 4].into()),
                    ),
                    (
                        "group_id".to_owned(),
                        Value::Uuid(ObjectId::from_uuid(row_id(1, resource % GROUPS).0)),
                    ),
                    ("held_by".to_owned(), Value::Null),
                    ("held_at".to_owned(), Value::Null),
                    ("heartbeat_at".to_owned(), Value::Timestamp(resource as u64)),
                    (
                        "version".to_owned(),
                        Value::Text(format!("v{}.{}", resource % 4, resource % 10)),
                    ),
                    ("latitude".to_owned(), Value::Double((resource % 90) as f64)),
                    (
                        "longitude".to_owned(),
                        Value::Double((resource % 180) as f64),
                    ),
                    (
                        "capacity".to_owned(),
                        Value::Double((resource % 100) as f64),
                    ),
                    ("uptime".to_owned(), Value::Integer(resource as i32)),
                ]),
            );
        }
    }

    fn insert_jobs(&mut self, count: usize) {
        for job in self.next_job..self.next_job + count {
            self.insert(
                "jobs",
                3,
                job,
                HashMap::from([
                    ("tenant".to_owned(), tenant(job)),
                    (
                        "group_id".to_owned(),
                        Value::Uuid(ObjectId::from_uuid(row_id(1, job % GROUPS).0)),
                    ),
                    ("resource_id".to_owned(), Value::Null),
                    (
                        "status".to_owned(),
                        Value::Text(["OPEN", "ASSIGNED", "DONE", "VERIFIED"][job % 4].into()),
                    ),
                    ("title".to_owned(), Value::Text(format!("Job {job:07}"))),
                    ("reward".to_owned(), Value::Integer((job % 10_000) as i32)),
                    (
                        "external_key".to_owned(),
                        Value::Text(format!("key-{job:07}")),
                    ),
                ]),
            );
        }
        self.next_job += count;
    }

    fn insert(&self, table: &str, kind: u8, index: usize, cells: HashMap<String, Value>) {
        let (_, _, transaction_id) = self
            .client
            .insert_with_id(table, row_id(kind, index).0, cells)
            .unwrap_or_else(|error| panic!("public-client insert synthetic {table} row: {error}"));
        block_on(self.client.wait_for_transaction(
            transaction_id.expect("public-client insert commits immediately"),
            jazz::tools::DurabilityTier::Local,
        ))
        .unwrap_or_else(|error| panic!("wait for public-client {table} row: {error}"));
    }
}

fn tenant(index: usize) -> Value {
    Value::Text(format!("tenant-{}", index % 2))
}

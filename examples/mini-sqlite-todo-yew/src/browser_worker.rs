use gloo_worker::{HandlerId, Worker, WorkerScope};
#[cfg(target_arch = "wasm32")]
use mini_jazz_sqlite::Storage;
use mini_jazz_sqlite::{sync::Bundle, BuiltQuery, RowView, Runtime, StorageStats};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type RuntimeRequestId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RuntimeWorkerInput {
    Open {
        db_name: String,
        node_id: String,
        user: String,
        hydrate_queries: Vec<BuiltQuery>,
    },
    ApplyBundles {
        request_id: RuntimeRequestId,
        bundles: Vec<Bundle>,
        refresh_queries: Vec<BuiltQuery>,
    },
    ExportQuery {
        request_id: RuntimeRequestId,
        query: BuiltQuery,
    },
    Query {
        request_id: RuntimeRequestId,
        query: BuiltQuery,
    },
    StorageStats {
        request_id: RuntimeRequestId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RuntimeWorkerOutput {
    Opened {
        bundles: Vec<Bundle>,
        storage_stats: BrowserStorageStats,
    },
    Applied {
        request_id: RuntimeRequestId,
        bundles: Vec<Bundle>,
        profile: WorkerSyncProfile,
        storage_stats: BrowserStorageStats,
    },
    Exported {
        request_id: RuntimeRequestId,
        bundle: Bundle,
    },
    QueryResult {
        request_id: RuntimeRequestId,
        rows: Vec<BrowserRowView>,
    },
    StorageStats {
        request_id: RuntimeRequestId,
        storage_stats: BrowserStorageStats,
    },
    Error {
        request_id: Option<RuntimeRequestId>,
        message: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkerSyncProfile {
    pub apply_ms: f64,
    pub refresh_query_ms: f64,
    pub refresh_export_ms: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserStorageStats {
    pub history_rows: i64,
    pub current_rows: i64,
    pub rejected_transactions: i64,
    pub page_count: i64,
    pub page_size: i64,
    pub database_bytes: i64,
    pub main_file_bytes: i64,
    pub wal_file_bytes: i64,
    pub shm_file_bytes: i64,
    pub total_file_bytes: i64,
    pub table_page_bytes: BTreeMap<String, i64>,
}

impl From<StorageStats> for BrowserStorageStats {
    fn from(stats: StorageStats) -> Self {
        Self {
            history_rows: stats.history_rows,
            current_rows: stats.current_rows,
            rejected_transactions: stats.rejected_transactions,
            page_count: stats.page_count,
            page_size: stats.page_size,
            database_bytes: stats.database_bytes,
            main_file_bytes: stats.main_file_bytes,
            wal_file_bytes: stats.wal_file_bytes,
            shm_file_bytes: stats.shm_file_bytes,
            total_file_bytes: stats.total_file_bytes,
            table_page_bytes: stats.table_page_bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserRowView {
    pub table: String,
    pub id: String,
    pub values: BTreeMap<String, Value>,
    pub created_by: String,
    pub tx_id: String,
    pub conflict_count: usize,
}

impl From<RowView> for BrowserRowView {
    fn from(row: RowView) -> Self {
        Self {
            table: row.table,
            id: row.id,
            values: row.values,
            created_by: row.created_by,
            tx_id: row.tx_id,
            conflict_count: row.conflict_count,
        }
    }
}

pub struct BrowserRuntimeWorker {
    runtime: Option<Runtime>,
}

pub enum BrowserRuntimeWorkerMessage {
    Opened {
        id: HandlerId,
        result: Result<(Runtime, Vec<Bundle>, BrowserStorageStats), String>,
    },
}

impl Worker for BrowserRuntimeWorker {
    type Message = BrowserRuntimeWorkerMessage;
    type Input = RuntimeWorkerInput;
    type Output = RuntimeWorkerOutput;

    fn create(_scope: &WorkerScope<Self>) -> Self {
        Self { runtime: None }
    }

    fn update(&mut self, scope: &WorkerScope<Self>, msg: Self::Message) {
        match msg {
            BrowserRuntimeWorkerMessage::Opened { id, result } => match result {
                Ok((runtime, bundles, storage_stats)) => {
                    self.runtime = Some(runtime);
                    scope.respond(
                        id,
                        RuntimeWorkerOutput::Opened {
                            bundles,
                            storage_stats,
                        },
                    );
                }
                Err(message) => scope.respond(
                    id,
                    RuntimeWorkerOutput::Error {
                        request_id: None,
                        message,
                    },
                ),
            },
        }
    }

    fn received(&mut self, scope: &WorkerScope<Self>, msg: Self::Input, id: HandlerId) {
        match msg {
            RuntimeWorkerInput::Open {
                db_name,
                node_id,
                user,
                hydrate_queries,
            } => {
                scope.send_future(async move {
                    BrowserRuntimeWorkerMessage::Opened {
                        id,
                        result: open_and_hydrate(db_name, node_id, user, hydrate_queries).await,
                    }
                });
            }
            RuntimeWorkerInput::ApplyBundles {
                request_id,
                bundles,
                refresh_queries,
            } => {
                let Some(runtime) = self.runtime.as_mut() else {
                    scope.respond(
                        id,
                        RuntimeWorkerOutput::Error {
                            request_id: Some(request_id),
                            message: "worker runtime is not ready".to_owned(),
                        },
                    );
                    return;
                };
                scope.respond(
                    id,
                    apply_bundles(runtime, request_id, bundles, refresh_queries),
                );
            }
            RuntimeWorkerInput::ExportQuery { request_id, query } => {
                let Some(runtime) = self.runtime.as_ref() else {
                    scope.respond(
                        id,
                        RuntimeWorkerOutput::Error {
                            request_id: Some(request_id),
                            message: "worker runtime is not ready".to_owned(),
                        },
                    );
                    return;
                };
                scope.respond(id, export_query(runtime, request_id, query));
            }
            RuntimeWorkerInput::Query { request_id, query } => {
                let Some(runtime) = self.runtime.as_ref() else {
                    scope.respond(
                        id,
                        RuntimeWorkerOutput::Error {
                            request_id: Some(request_id),
                            message: "worker runtime is not ready".to_owned(),
                        },
                    );
                    return;
                };
                scope.respond(id, run_query(runtime, request_id, query));
            }
            RuntimeWorkerInput::StorageStats { request_id } => {
                let Some(runtime) = self.runtime.as_ref() else {
                    scope.respond(
                        id,
                        RuntimeWorkerOutput::Error {
                            request_id: Some(request_id),
                            message: "worker runtime is not ready".to_owned(),
                        },
                    );
                    return;
                };
                scope.respond(id, storage_stats(runtime, request_id));
            }
        }
    }
}

async fn open_and_hydrate(
    db_name: String,
    node_id: String,
    user: String,
    hydrate_queries: Vec<BuiltQuery>,
) -> Result<(Runtime, Vec<Bundle>, BrowserStorageStats), String> {
    let runtime = open_opfs_runtime(&db_name, &node_id, &user).await?;
    let bundles = hydrate_queries
        .into_iter()
        .map(|query| runtime.export_query(query).map_err(error_message))
        .collect::<Result<Vec<_>, _>>()?;
    let storage_stats = runtime.storage_stats().map_err(error_message)?.into();
    Ok((runtime, bundles, storage_stats))
}

fn apply_bundles(
    runtime: &mut Runtime,
    request_id: RuntimeRequestId,
    bundles: Vec<Bundle>,
    refresh_queries: Vec<BuiltQuery>,
) -> RuntimeWorkerOutput {
    match apply_and_refresh(runtime, bundles, refresh_queries) {
        Ok((bundles, profile, storage_stats)) => RuntimeWorkerOutput::Applied {
            request_id,
            bundles,
            profile,
            storage_stats,
        },
        Err(message) => RuntimeWorkerOutput::Error {
            request_id: Some(request_id),
            message,
        },
    }
}

fn apply_and_refresh(
    runtime: &mut Runtime,
    bundles: Vec<Bundle>,
    refresh_queries: Vec<BuiltQuery>,
) -> Result<(Vec<Bundle>, WorkerSyncProfile, BrowserStorageStats), String> {
    let apply_started_at = now_ms();
    for bundle in bundles {
        runtime.apply_bundle(&bundle).map_err(error_message)?;
    }
    let apply_ms = now_ms() - apply_started_at;

    let query_started_at = now_ms();
    for query in &refresh_queries {
        runtime.query(query.clone()).map_err(error_message)?;
    }
    let refresh_query_ms = now_ms() - query_started_at;

    let export_started_at = now_ms();
    let bundles = refresh_queries
        .into_iter()
        .map(|query| runtime.export_query(query).map_err(error_message))
        .collect::<Result<Vec<_>, _>>()?;
    let refresh_export_ms = now_ms() - export_started_at;

    let storage_stats = runtime.storage_stats().map_err(error_message)?.into();
    Ok((
        bundles,
        WorkerSyncProfile {
            apply_ms,
            refresh_query_ms,
            refresh_export_ms,
        },
        storage_stats,
    ))
}

fn export_query(
    runtime: &Runtime,
    request_id: RuntimeRequestId,
    query: BuiltQuery,
) -> RuntimeWorkerOutput {
    match runtime.export_query(query).map_err(error_message) {
        Ok(bundle) => RuntimeWorkerOutput::Exported { request_id, bundle },
        Err(message) => RuntimeWorkerOutput::Error {
            request_id: Some(request_id),
            message,
        },
    }
}

fn run_query(
    runtime: &Runtime,
    request_id: RuntimeRequestId,
    query: BuiltQuery,
) -> RuntimeWorkerOutput {
    match runtime.query(query).map_err(error_message) {
        Ok(rows) => RuntimeWorkerOutput::QueryResult {
            request_id,
            rows: rows.into_iter().map(Into::into).collect(),
        },
        Err(message) => RuntimeWorkerOutput::Error {
            request_id: Some(request_id),
            message,
        },
    }
}

fn storage_stats(runtime: &Runtime, request_id: RuntimeRequestId) -> RuntimeWorkerOutput {
    match runtime.storage_stats().map_err(error_message) {
        Ok(stats) => RuntimeWorkerOutput::StorageStats {
            request_id,
            storage_stats: stats.into(),
        },
        Err(message) => RuntimeWorkerOutput::Error {
            request_id: Some(request_id),
            message,
        },
    }
}

#[cfg(target_arch = "wasm32")]
async fn open_opfs_runtime(db_name: &str, node_id: &str, user: &str) -> Result<Runtime, String> {
    let pool_name = opfs_pool_name(db_name);
    let pool_directory = format!(".{pool_name}");
    let config = sqlite_wasm_vfs::sahpool::OpfsSAHPoolCfgBuilder::new()
        .vfs_name(&pool_name)
        .directory(&pool_directory)
        .build();
    sqlite_wasm_vfs::sahpool::install::<sqlite_wasm_rs::WasmOsCallback>(&config, true)
        .await
        .map_err(|error| format!("install OPFS SQLite VFS: {error}"))?;

    Runtime::open(Storage::File(db_name.into()), node_id, user).map_err(error_message)
}

#[cfg(not(target_arch = "wasm32"))]
async fn open_opfs_runtime(_db_name: &str, _node_id: &str, _user: &str) -> Result<Runtime, String> {
    Err("OPFS SQLite is only available in wasm".to_owned())
}

#[cfg(target_arch = "wasm32")]
fn now_ms() -> f64 {
    js_sys::Date::now()
}

#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> f64 {
    0.0
}

#[cfg(target_arch = "wasm32")]
fn opfs_pool_name(db_name: &str) -> String {
    let mut name = String::from("mini-jazz-sqlite-");
    for ch in db_name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            name.push(ch);
        } else {
            name.push('-');
        }
    }
    name
}

fn error_message(error: mini_jazz_sqlite::Error) -> String {
    error.to_string()
}

#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

#[cfg(target_arch = "wasm32")]
use crate::worker_bridge::WorkerResponder;
#[cfg(target_arch = "wasm32")]
use mini_jazz_sqlite::Storage;
use mini_jazz_sqlite::{sync::Bundle, BuiltQuery, RowView, Runtime, SchemaDef, StorageStats};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
#[cfg(target_arch = "wasm32")]
use std::{cell::RefCell, rc::Rc};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

pub type RuntimeRequestId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RuntimeWorkerInput {
    Open {
        db_name: String,
        node_id: String,
        user: String,
        schema: SchemaDef,
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
    ExportQueries {
        request_id: RuntimeRequestId,
        queries: Vec<BuiltQuery>,
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
    ExportedQueries {
        request_id: RuntimeRequestId,
        bundles: Vec<Bundle>,
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
    pub created_at: i64,
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
            created_at: row.created_at,
            created_by: row.created_by,
            tx_id: row.tx_id,
            conflict_count: row.conflict_count,
        }
    }
}

pub struct BrowserRuntimeWorker {
    runtime: Option<Runtime>,
}

impl BrowserRuntimeWorker {
    pub fn new() -> Self {
        Self { runtime: None }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn handle_shared(
        worker: Rc<RefCell<Self>>,
        msg: RuntimeWorkerInput,
        responder: WorkerResponder<RuntimeWorkerOutput>,
    ) {
        match msg {
            RuntimeWorkerInput::Open {
                db_name,
                node_id,
                user,
                schema,
                hydrate_queries,
            } => {
                spawn_local(async move {
                    let output =
                        match open_and_hydrate(db_name, node_id, user, schema, hydrate_queries)
                            .await
                        {
                            Ok((runtime, bundles, storage_stats)) => {
                                worker.borrow_mut().runtime = Some(runtime);
                                RuntimeWorkerOutput::Opened {
                                    bundles,
                                    storage_stats,
                                }
                            }
                            Err(message) => RuntimeWorkerOutput::Error {
                                request_id: None,
                                message,
                            },
                        };
                    responder.send(output);
                });
            }
            msg => responder.send(worker.borrow_mut().handle_sync(msg)),
        }
    }

    fn handle_sync(&mut self, msg: RuntimeWorkerInput) -> RuntimeWorkerOutput {
        match msg {
            RuntimeWorkerInput::Open { .. } => RuntimeWorkerOutput::Error {
                request_id: None,
                message: "open must be handled asynchronously".to_owned(),
            },
            RuntimeWorkerInput::ApplyBundles {
                request_id,
                bundles,
                refresh_queries,
            } => {
                let Some(runtime) = self.runtime.as_mut() else {
                    return runtime_not_ready(request_id);
                };
                apply_bundles(runtime, request_id, bundles, refresh_queries)
            }
            RuntimeWorkerInput::ExportQuery { request_id, query } => {
                let Some(runtime) = self.runtime.as_ref() else {
                    return runtime_not_ready(request_id);
                };
                export_query(runtime, request_id, query)
            }
            RuntimeWorkerInput::ExportQueries {
                request_id,
                queries,
            } => {
                let Some(runtime) = self.runtime.as_ref() else {
                    return runtime_not_ready(request_id);
                };
                export_queries(runtime, request_id, queries)
            }
            RuntimeWorkerInput::Query { request_id, query } => {
                let Some(runtime) = self.runtime.as_ref() else {
                    return runtime_not_ready(request_id);
                };
                run_query(runtime, request_id, query)
            }
            RuntimeWorkerInput::StorageStats { request_id } => {
                let Some(runtime) = self.runtime.as_ref() else {
                    return runtime_not_ready(request_id);
                };
                storage_stats(runtime, request_id)
            }
        }
    }
}

impl Default for BrowserRuntimeWorker {
    fn default() -> Self {
        Self::new()
    }
}

fn runtime_not_ready(request_id: RuntimeRequestId) -> RuntimeWorkerOutput {
    RuntimeWorkerOutput::Error {
        request_id: Some(request_id),
        message: "worker runtime is not ready".to_owned(),
    }
}

async fn open_and_hydrate(
    db_name: String,
    node_id: String,
    user: String,
    schema: SchemaDef,
    hydrate_queries: Vec<BuiltQuery>,
) -> Result<(Runtime, Vec<Bundle>, BrowserStorageStats), String> {
    let runtime = open_opfs_runtime(&db_name, &node_id, &user, schema).await?;
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

fn export_queries(
    runtime: &Runtime,
    request_id: RuntimeRequestId,
    queries: Vec<BuiltQuery>,
) -> RuntimeWorkerOutput {
    match queries
        .into_iter()
        .map(|query| runtime.export_query(query).map_err(error_message))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(bundles) => RuntimeWorkerOutput::ExportedQueries {
            request_id,
            bundles,
        },
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
async fn open_opfs_runtime(
    db_name: &str,
    node_id: &str,
    user: &str,
    schema: SchemaDef,
) -> Result<Runtime, String> {
    let pool_name = opfs_pool_name(db_name);
    let pool_directory = format!(".{pool_name}");
    let config = sqlite_wasm_vfs::sahpool::OpfsSAHPoolCfgBuilder::new()
        .vfs_name(&pool_name)
        .directory(&pool_directory)
        .build();
    sqlite_wasm_vfs::sahpool::install::<sqlite_wasm_rs::WasmOsCallback>(&config, true)
        .await
        .map_err(|error| format!("install OPFS SQLite VFS: {error}"))?;

    Runtime::open_with_schema(Storage::File(db_name.into()), node_id, user, schema)
        .map_err(error_message)
}

#[cfg(not(target_arch = "wasm32"))]
async fn open_opfs_runtime(
    _db_name: &str,
    _node_id: &str,
    _user: &str,
    _schema: SchemaDef,
) -> Result<Runtime, String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::todo_schema::todo_schema;
    use mini_jazz_sqlite::{BuiltQuery, QueryCondition, QueryConditionOp, Runtime, Storage};
    use serde::de::DeserializeOwned;
    use serde_json::json;

    #[test]
    fn worker_open_message_round_trips_through_serde_json() {
        let message = RuntimeWorkerInput::Open {
            db_name: "todo.sqlite3".to_owned(),
            node_id: "worker".to_owned(),
            user: "alice".to_owned(),
            schema: todo_schema(),
            hydrate_queries: vec![BuiltQuery {
                table: "todos".to_owned(),
                conditions: vec![QueryCondition {
                    column: "title".to_owned(),
                    op: QueryConditionOp::Contains,
                    value: json!("bridge"),
                }],
                order_by: Vec::new(),
                limit: Some(10),
                offset: None,
            }],
        };

        let decoded: RuntimeWorkerInput = serde_round_trip(&message);

        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }

    #[test]
    fn worker_apply_message_round_trips_query_and_bundle_values_through_serde_json() {
        let runtime =
            Runtime::open_with_schema(Storage::Memory, "main", "alice", todo_schema()).unwrap();
        let bundle = runtime
            .export_query(BuiltQuery {
                table: "todos".to_owned(),
                conditions: vec![QueryCondition {
                    column: "done".to_owned(),
                    op: QueryConditionOp::Eq,
                    value: json!(false),
                }],
                order_by: Vec::new(),
                limit: Some(10),
                offset: None,
            })
            .unwrap();
        let message = RuntimeWorkerInput::ApplyBundles {
            request_id: 42,
            bundles: vec![bundle],
            refresh_queries: vec![BuiltQuery {
                table: "todos".to_owned(),
                conditions: vec![QueryCondition {
                    column: "labels".to_owned(),
                    op: QueryConditionOp::In,
                    value: json!(["work", {"nested": true}]),
                }],
                order_by: Vec::new(),
                limit: Some(10),
                offset: None,
            }],
        };

        let decoded: RuntimeWorkerInput = serde_round_trip(&message);

        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }

    #[test]
    fn worker_export_queries_message_round_trips_multiple_queries_through_serde_json() {
        let message = RuntimeWorkerInput::ExportQueries {
            request_id: 11,
            queries: vec![
                BuiltQuery {
                    table: "todos".to_owned(),
                    conditions: vec![QueryCondition {
                        column: "done".to_owned(),
                        op: QueryConditionOp::Eq,
                        value: json!(false),
                    }],
                    order_by: Vec::new(),
                    limit: Some(10),
                    offset: Some(0),
                },
                BuiltQuery {
                    table: "todos".to_owned(),
                    conditions: Vec::new(),
                    order_by: Vec::new(),
                    limit: Some(1),
                    offset: Some(10),
                },
            ],
        };

        let decoded: RuntimeWorkerInput = serde_round_trip(&message);

        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }

    #[test]
    fn worker_query_result_round_trips_rows_through_serde_json() {
        let message = RuntimeWorkerOutput::QueryResult {
            request_id: 7,
            rows: vec![BrowserRowView {
                table: "todos".to_owned(),
                id: "todo-1".to_owned(),
                values: BTreeMap::from([
                    ("title".to_owned(), json!("Write bridge")),
                    ("done".to_owned(), json!(false)),
                    ("labels".to_owned(), json!(["work", "rust"])),
                ]),
                created_at: 123,
                created_by: "alice".to_owned(),
                tx_id: "tx-1".to_owned(),
                conflict_count: 0,
            }],
        };

        let decoded: RuntimeWorkerOutput = serde_round_trip(&message);

        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }

    fn serde_round_trip<T>(value: &T) -> T
    where
        T: Serialize + DeserializeOwned,
    {
        serde_json::from_str(&serde_json::to_string(value).unwrap()).unwrap()
    }
}

#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

#[cfg(target_arch = "wasm32")]
use crate::worker_bridge::WorkerResponder;
use mini_jazz_sqlite::connection::UpstreamConnectionManager;
use mini_jazz_sqlite::protocol::{ClientMessage, ServerMessage};
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
        client_messages: Vec<ClientMessage>,
    },
    ApplyBundles {
        request_id: RuntimeRequestId,
        bundles: Vec<Bundle>,
        client_messages: Vec<ClientMessage>,
    },
    Protocol {
        request_id: RuntimeRequestId,
        client_messages: Vec<ClientMessage>,
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
        server_messages: Vec<ServerMessage>,
        storage_stats: BrowserStorageStats,
    },
    Applied {
        request_id: RuntimeRequestId,
        server_messages: Vec<ServerMessage>,
        storage_stats: BrowserStorageStats,
    },
    Protocol {
        request_id: RuntimeRequestId,
        server_messages: Vec<ServerMessage>,
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
    upstream_connection_manager: Option<UpstreamConnectionManager>,
}

impl BrowserRuntimeWorker {
    pub fn new() -> Self {
        Self {
            runtime: None,
            upstream_connection_manager: None,
        }
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
                client_messages,
            } => {
                spawn_local(async move {
                    let output = match open_and_hydrate(
                        db_name,
                        node_id,
                        user,
                        schema,
                        hydrate_queries,
                        client_messages,
                    )
                    .await
                    {
                        Ok((
                            runtime,
                            upstream_connection_manager,
                            bundles,
                            server_messages,
                            storage_stats,
                        )) => {
                            worker.borrow_mut().runtime = Some(runtime);
                            worker.borrow_mut().upstream_connection_manager =
                                Some(upstream_connection_manager);
                            RuntimeWorkerOutput::Opened {
                                bundles,
                                server_messages,
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
                client_messages,
            } => {
                let Some(runtime) = self.runtime.as_mut() else {
                    return runtime_not_ready(request_id);
                };
                let Some(upstream_connection_manager) = self.upstream_connection_manager.as_mut()
                else {
                    return runtime_not_ready(request_id);
                };
                apply_bundles(
                    runtime,
                    upstream_connection_manager,
                    request_id,
                    bundles,
                    client_messages,
                )
            }
            RuntimeWorkerInput::Protocol {
                request_id,
                client_messages,
            } => {
                let Some(runtime) = self.runtime.as_mut() else {
                    return runtime_not_ready(request_id);
                };
                let Some(upstream_connection_manager) = self.upstream_connection_manager.as_mut()
                else {
                    return runtime_not_ready(request_id);
                };
                protocol_messages(
                    runtime,
                    upstream_connection_manager,
                    request_id,
                    client_messages,
                )
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
    client_messages: Vec<ClientMessage>,
) -> Result<
    (
        Runtime,
        UpstreamConnectionManager,
        Vec<Bundle>,
        Vec<ServerMessage>,
        BrowserStorageStats,
    ),
    String,
> {
    let mut runtime = open_opfs_runtime(&db_name, &node_id, &user, schema).await?;
    let mut upstream_connection_manager = UpstreamConnectionManager::new(
        format!("{node_id}-session"),
        node_id.clone(),
        runtime.local_schema_fingerprint(),
        runtime.local_policy_fingerprint(),
    );
    let bundles = hydrate_queries
        .into_iter()
        .map(|query| runtime.export_query(query).map_err(error_message))
        .collect::<Result<Vec<_>, _>>()?;
    let server_messages = pump_upstream_connection_manager(
        &mut runtime,
        &mut upstream_connection_manager,
        client_messages,
    )?;
    let storage_stats = runtime.storage_stats().map_err(error_message)?.into();
    Ok((
        runtime,
        upstream_connection_manager,
        bundles,
        server_messages,
        storage_stats,
    ))
}

fn apply_bundles(
    runtime: &mut Runtime,
    upstream_connection_manager: &mut UpstreamConnectionManager,
    request_id: RuntimeRequestId,
    bundles: Vec<Bundle>,
    client_messages: Vec<ClientMessage>,
) -> RuntimeWorkerOutput {
    match apply_and_refresh(
        runtime,
        upstream_connection_manager,
        bundles,
        client_messages,
    ) {
        Ok((server_messages, storage_stats)) => RuntimeWorkerOutput::Applied {
            request_id,
            server_messages,
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
    upstream_connection_manager: &mut UpstreamConnectionManager,
    bundles: Vec<Bundle>,
    client_messages: Vec<ClientMessage>,
) -> Result<(Vec<ServerMessage>, BrowserStorageStats), String> {
    for bundle in bundles {
        runtime.apply_bundle(&bundle).map_err(error_message)?;
    }

    let server_messages =
        pump_upstream_connection_manager(runtime, upstream_connection_manager, client_messages)?;
    let storage_stats = runtime.storage_stats().map_err(error_message)?.into();
    Ok((server_messages, storage_stats))
}

fn protocol_messages(
    runtime: &mut Runtime,
    upstream_connection_manager: &mut UpstreamConnectionManager,
    request_id: RuntimeRequestId,
    client_messages: Vec<ClientMessage>,
) -> RuntimeWorkerOutput {
    match pump_upstream_connection_manager(runtime, upstream_connection_manager, client_messages) {
        Ok(server_messages) => {
            let storage_stats = match runtime.storage_stats().map_err(error_message) {
                Ok(stats) => stats.into(),
                Err(message) => {
                    return RuntimeWorkerOutput::Error {
                        request_id: Some(request_id),
                        message,
                    };
                }
            };
            RuntimeWorkerOutput::Protocol {
                request_id,
                server_messages,
                storage_stats,
            }
        }
        Err(message) => RuntimeWorkerOutput::Error {
            request_id: Some(request_id),
            message,
        },
    }
}

fn pump_upstream_connection_manager(
    runtime: &mut Runtime,
    upstream_connection_manager: &mut UpstreamConnectionManager,
    client_messages: Vec<ClientMessage>,
) -> Result<Vec<ServerMessage>, String> {
    upstream_connection_manager
        .receive(runtime, client_messages)
        .map_err(error_message)
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
            client_messages: Vec::new(),
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
            client_messages: vec![mini_jazz_sqlite::protocol::ClientMessage::Replay {
                subscriptions: vec![mini_jazz_sqlite::protocol::ReplaySubscription {
                    subscription_id: mini_jazz_sqlite::protocol::SubscriptionId::new(
                        "browser-subscription-1",
                    ),
                    query: BuiltQuery {
                        table: "todos".to_owned(),
                        conditions: vec![QueryCondition {
                            column: "labels".to_owned(),
                            op: QueryConditionOp::In,
                            value: json!(["work", {"nested": true}]),
                        }],
                        order_by: Vec::new(),
                        limit: Some(10),
                        offset: None,
                    },
                    requested_tier: mini_jazz_sqlite::protocol::SettlementTier::Local,
                    last_applied_cursor: None,
                }],
            }],
        };

        let decoded: RuntimeWorkerInput = serde_round_trip(&message);

        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }

    #[test]
    fn worker_protocol_message_round_trips_through_serde_json() {
        let message = RuntimeWorkerInput::Protocol {
            request_id: 99,
            client_messages: vec![mini_jazz_sqlite::protocol::ClientMessage::Close(
                mini_jazz_sqlite::protocol::CloseReason::ClientClosed,
            )],
        };

        let decoded: RuntimeWorkerInput = serde_round_trip(&message);

        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );

        let output = RuntimeWorkerOutput::Protocol {
            request_id: 99,
            server_messages: vec![mini_jazz_sqlite::protocol::ServerMessage::Close(
                mini_jazz_sqlite::protocol::CloseReason::ClientClosed,
            )],
            storage_stats: BrowserStorageStats::default(),
        };
        let decoded: RuntimeWorkerOutput = serde_round_trip(&output);

        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(output).unwrap()
        );
    }

    #[test]
    fn worker_applied_output_deserializes_minimal_payload() {
        let message = json!({
            "Applied": {
                "request_id": 7,
                "server_messages": [],
                "storage_stats": BrowserStorageStats::default()
            }
        });

        let decoded: RuntimeWorkerOutput = serde_json::from_value(message).unwrap();

        let RuntimeWorkerOutput::Applied {
            request_id,
            server_messages,
            storage_stats,
            ..
        } = decoded
        else {
            panic!("expected applied output");
        };
        assert_eq!(request_id, 7);
        assert!(server_messages.is_empty());
        assert_eq!(storage_stats, BrowserStorageStats::default());
    }

    #[test]
    fn worker_protocol_subscribe_exports_query_data_and_settlement() {
        use mini_jazz_sqlite::protocol::{
            ClientHello, ClientMessage, ProtocolVersion, ServerMessage, SessionId, SettlementTier,
            SubscriptionId,
        };

        let mut runtime =
            Runtime::open_with_schema(Storage::Memory, "worker", "alice", todo_schema()).unwrap();
        runtime
            .insert_row(
                "projects",
                "project-1",
                BTreeMap::from([("title".to_owned(), json!("Launch"))]),
            )
            .unwrap();
        runtime
            .insert_row(
                "todos",
                "todo-1",
                BTreeMap::from([
                    ("title".to_owned(), json!("Use protocol")),
                    ("done".to_owned(), json!(false)),
                    ("project".to_owned(), json!("project-1")),
                ]),
            )
            .unwrap();
        let schema_fingerprint = runtime.local_schema_fingerprint();
        let policy_fingerprint = runtime.local_policy_fingerprint();
        let query = BuiltQuery {
            table: "todos".to_owned(),
            conditions: vec![QueryCondition {
                column: "done".to_owned(),
                op: QueryConditionOp::Eq,
                value: json!(false),
            }],
            order_by: Vec::new(),
            limit: Some(10),
            offset: None,
        };
        let subscription_id = SubscriptionId::new("browser-subscription-1");
        let mut worker = BrowserRuntimeWorker {
            runtime: Some(runtime),
            upstream_connection_manager: Some(UpstreamConnectionManager::new(
                "worker-session",
                "worker",
                schema_fingerprint.clone(),
                policy_fingerprint.clone(),
            )),
        };

        let output = worker.handle_sync(RuntimeWorkerInput::Protocol {
            request_id: 7,
            client_messages: vec![
                ClientMessage::Hello(ClientHello {
                    protocol_version: ProtocolVersion(1),
                    session_id: SessionId::new("browser-session"),
                    node_id: "browser".to_owned(),
                    schema_fingerprint,
                    policy_fingerprint,
                }),
                ClientMessage::Subscribe {
                    subscription_id: subscription_id.clone(),
                    query,
                    requested_tier: SettlementTier::Local,
                },
            ],
        });

        let RuntimeWorkerOutput::Protocol {
            request_id,
            server_messages,
            ..
        } = output
        else {
            panic!("expected protocol output");
        };
        assert_eq!(request_id, 7);
        assert!(matches!(server_messages[0], ServerMessage::Hello(_)));
        assert!(matches!(
            &server_messages[1],
            ServerMessage::Data {
                subscription_id: Some(id),
                ..
            } if id == &subscription_id
        ));
        assert!(matches!(
            &server_messages[2],
            ServerMessage::Settled {
                subscription_id: id,
                tier: SettlementTier::Local,
                ..
            } if id == &subscription_id
        ));
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

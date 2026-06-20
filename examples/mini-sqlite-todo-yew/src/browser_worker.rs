#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

#[cfg(target_arch = "wasm32")]
use crate::native_sync::{decode_server_frame, encode_client_frame};
#[cfg(target_arch = "wasm32")]
use crate::worker_bridge::WorkerResponder;
use mini_jazz_sqlite::connection::UpstreamConnectionManager;
#[cfg(target_arch = "wasm32")]
use mini_jazz_sqlite::protocol::{ClientHello, SessionId, SUPPORTED_PROTOCOL_VERSION};
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
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;
#[cfg(target_arch = "wasm32")]
use web_sys::{CloseEvent, ErrorEvent, Event, MessageEvent, WebSocket};

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
        native_sync_url: Option<String>,
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
    Pushed {
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
    native_sync: Option<NativeSync>,
}

struct NativeSync {
    node_id: String,
    schema_fingerprint: String,
    policy_fingerprint: String,
    ready: bool,
    sent_hello: bool,
    pending_client_messages: Vec<ClientMessage>,
    #[cfg(target_arch = "wasm32")]
    socket: WebSocket,
    #[cfg(target_arch = "wasm32")]
    _onopen: Closure<dyn FnMut(Event)>,
    #[cfg(target_arch = "wasm32")]
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
    #[cfg(target_arch = "wasm32")]
    _onerror: Closure<dyn FnMut(ErrorEvent)>,
    #[cfg(target_arch = "wasm32")]
    _onclose: Closure<dyn FnMut(CloseEvent)>,
}

impl BrowserRuntimeWorker {
    pub fn new() -> Self {
        Self {
            runtime: None,
            upstream_connection_manager: None,
            native_sync: None,
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
                native_sync_url,
            } => {
                spawn_local(async move {
                    let native_node_id = node_id.clone();
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
                            let native_sync =
                                native_sync_url.and_then(|url| {
                                    match NativeSync::open(
                                        worker.clone(),
                                        responder.clone(),
                                        url,
                                        native_node_id.clone(),
                                        runtime.local_schema_fingerprint(),
                                        runtime.local_policy_fingerprint(),
                                    ) {
                                        Ok(sync) => Some(sync),
                                        Err(message) => {
                                            responder.send(RuntimeWorkerOutput::Error {
                                                request_id: None,
                                                message,
                                            });
                                            None
                                        }
                                    }
                                });
                            if let Some(mut sync) = native_sync {
                                let _ = sync.flush_pending();
                                worker.borrow_mut().native_sync = Some(sync);
                            }
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
                let native_client_messages = relayable_native_client_messages(&client_messages);
                let output = apply_bundles(
                    runtime,
                    upstream_connection_manager,
                    request_id,
                    bundles,
                    client_messages,
                );
                if let Err(message) = self.send_native_client_messages(native_client_messages) {
                    return RuntimeWorkerOutput::Error {
                        request_id: Some(request_id),
                        message,
                    };
                }
                output
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
                let native_client_messages = relayable_native_client_messages(&client_messages);
                let output = protocol_messages(
                    runtime,
                    upstream_connection_manager,
                    request_id,
                    client_messages,
                );
                if let Err(message) = self.send_native_client_messages(native_client_messages) {
                    return RuntimeWorkerOutput::Error {
                        request_id: Some(request_id),
                        message,
                    };
                }
                output
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

impl BrowserRuntimeWorker {
    fn send_native_client_messages(
        &mut self,
        client_messages: Vec<ClientMessage>,
    ) -> Result<(), String> {
        if client_messages.is_empty() {
            return Ok(());
        }
        let Some(sync) = self.native_sync.as_mut() else {
            return Ok(());
        };
        sync.send_or_buffer(client_messages)
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
    let mut upstream_connection_manager = UpstreamConnectionManager::new_authenticated(
        format!("{node_id}-session"),
        node_id.clone(),
        runtime.local_schema_fingerprint(),
        runtime.local_policy_fingerprint(),
        user,
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

fn relayable_native_client_messages(client_messages: &[ClientMessage]) -> Vec<ClientMessage> {
    client_messages
        .iter()
        .filter_map(|message| match message {
            ClientMessage::Subscribe { .. }
            | ClientMessage::Replay { .. }
            | ClientMessage::UploadTx { .. }
            | ClientMessage::Unsubscribe { .. } => Some(message.clone()),
            ClientMessage::Hello(_) | ClientMessage::Ack { .. } | ClientMessage::Close(_) => None,
        })
        .collect()
}

fn apply_native_server_messages(
    runtime: &mut Runtime,
    upstream_connection_manager: &mut UpstreamConnectionManager,
    server_messages: Vec<ServerMessage>,
) -> Result<(Vec<ClientMessage>, Vec<ServerMessage>, BrowserStorageStats), String> {
    let mut client_messages = Vec::new();
    for message in server_messages {
        match message {
            ServerMessage::Hello(_) => {}
            ServerMessage::Data {
                message_id,
                cursor,
                bundle,
                ..
            } => {
                runtime.apply_bundle(&bundle).map_err(error_message)?;
                client_messages.push(ClientMessage::Ack {
                    message_id,
                    cursor: Some(cursor),
                });
            }
            ServerMessage::TxStatus { tx_id, status } => {
                runtime
                    .apply_tx_status_from_server(&tx_id, status)
                    .map_err(error_message)?;
            }
            ServerMessage::UploadAck { .. } | ServerMessage::Settled { .. } => {}
            ServerMessage::Error(error) => {
                return Err(format!(
                    "native sync error {}: {}",
                    error.code, error.message
                ));
            }
            ServerMessage::Close(reason) => {
                return Err(format!("native sync closed: {reason:?}"));
            }
        }
    }
    let main_server_messages = upstream_connection_manager
        .refresh_active_subscriptions(runtime)
        .map_err(error_message)?;
    let storage_stats = runtime.storage_stats().map_err(error_message)?.into();
    Ok((client_messages, main_server_messages, storage_stats))
}

impl NativeSync {
    #[cfg(target_arch = "wasm32")]
    fn open(
        worker: Rc<RefCell<BrowserRuntimeWorker>>,
        responder: WorkerResponder<RuntimeWorkerOutput>,
        url: String,
        node_id: String,
        schema_fingerprint: String,
        policy_fingerprint: String,
    ) -> Result<Self, String> {
        let socket =
            WebSocket::new(&url).map_err(|error| format!("open native sync: {error:?}"))?;

        let onopen = Closure::wrap(Box::new({
            let worker = worker.clone();
            move |_event: Event| {
                if let Some(sync) = worker.borrow_mut().native_sync.as_mut() {
                    sync.ready = true;
                    if let Err(message) = sync.flush_pending() {
                        web_sys::console::error_1(&JsValue::from_str(&message));
                    }
                }
            }
        }) as Box<dyn FnMut(Event)>);

        let onmessage = Closure::wrap(Box::new({
            let worker = worker.clone();
            let responder = responder.clone();
            move |event: MessageEvent| {
                let Some(encoded) = event.data().as_string() else {
                    responder.send(RuntimeWorkerOutput::Error {
                        request_id: None,
                        message: "native sync sent a non-string frame".to_owned(),
                    });
                    return;
                };
                let frame = match decode_server_frame(&encoded) {
                    Ok(frame) => frame,
                    Err(message) => {
                        responder.send(RuntimeWorkerOutput::Error {
                            request_id: None,
                            message,
                        });
                        return;
                    }
                };
                let output = {
                    let mut worker = worker.borrow_mut();
                    let result = {
                        let worker = &mut *worker;
                        let (Some(runtime), Some(upstream_connection_manager)) = (
                            worker.runtime.as_mut(),
                            worker.upstream_connection_manager.as_mut(),
                        ) else {
                            return;
                        };
                        apply_native_server_messages(
                            runtime,
                            upstream_connection_manager,
                            frame.server_messages,
                        )
                    };
                    match result {
                        Ok((client_messages, main_server_messages, storage_stats)) => {
                            if let Some(sync) = worker.native_sync.as_mut() {
                                if let Err(message) = sync.send_or_buffer(client_messages) {
                                    return responder.send(RuntimeWorkerOutput::Error {
                                        request_id: None,
                                        message,
                                    });
                                }
                            }
                            if main_server_messages.is_empty() {
                                return;
                            }
                            RuntimeWorkerOutput::Pushed {
                                server_messages: main_server_messages,
                                storage_stats,
                            }
                        }
                        Err(message) => RuntimeWorkerOutput::Error {
                            request_id: None,
                            message,
                        },
                    }
                };
                responder.send(output);
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        let onerror = Closure::wrap(Box::new({
            let responder = responder.clone();
            move |_event: ErrorEvent| {
                responder.send(RuntimeWorkerOutput::Error {
                    request_id: None,
                    message: "native sync websocket error".to_owned(),
                });
            }
        }) as Box<dyn FnMut(ErrorEvent)>);

        let onclose = Closure::wrap(Box::new({
            let worker = worker.clone();
            move |_event: CloseEvent| {
                if let Some(sync) = worker.borrow_mut().native_sync.as_mut() {
                    sync.ready = false;
                }
            }
        }) as Box<dyn FnMut(CloseEvent)>);

        socket.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        socket.set_onerror(Some(onerror.as_ref().unchecked_ref()));
        socket.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        Ok(Self {
            node_id,
            schema_fingerprint,
            policy_fingerprint,
            ready: false,
            sent_hello: false,
            pending_client_messages: Vec::new(),
            socket,
            _onopen: onopen,
            _onmessage: onmessage,
            _onerror: onerror,
            _onclose: onclose,
        })
    }

    fn send_or_buffer(&mut self, client_messages: Vec<ClientMessage>) -> Result<(), String> {
        if client_messages.is_empty() {
            return Ok(());
        }
        self.pending_client_messages.extend(client_messages);
        self.flush_pending()
    }

    fn flush_pending(&mut self) -> Result<(), String> {
        #[cfg(target_arch = "wasm32")]
        {
            if !self.ready {
                return Ok(());
            }
            let mut client_messages = Vec::new();
            if !self.sent_hello {
                client_messages.push(ClientMessage::Hello(ClientHello {
                    protocol_version: SUPPORTED_PROTOCOL_VERSION,
                    session_id: SessionId::new(format!("{}-native-session", self.node_id)),
                    node_id: self.node_id.clone(),
                    schema_fingerprint: self.schema_fingerprint.clone(),
                    policy_fingerprint: self.policy_fingerprint.clone(),
                }));
                self.sent_hello = true;
            }
            client_messages.append(&mut self.pending_client_messages);
            if client_messages.is_empty() {
                return Ok(());
            }
            let encoded = encode_client_frame(client_messages)?;
            self.socket
                .send_with_str(&encoded)
                .map_err(|error| format!("send native sync frame: {error:?}"))?;
        }
        Ok(())
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
            native_sync_url: Some("ws://127.0.0.1:8787/sync".to_owned()),
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

        let output = RuntimeWorkerOutput::Pushed {
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
    fn worker_relays_subscriptions_and_uploads_to_native_sync() {
        use mini_jazz_sqlite::protocol::{
            ClientMessage, ClientTx, DataOp, SettlementTier, SubscriptionId, TxConflictMode,
        };

        let messages = vec![
            ClientMessage::Hello(mini_jazz_sqlite::protocol::ClientHello {
                protocol_version: mini_jazz_sqlite::protocol::ProtocolVersion(2),
                session_id: mini_jazz_sqlite::protocol::SessionId::new("browser-session"),
                node_id: "browser".to_owned(),
                schema_fingerprint: "schema".to_owned(),
                policy_fingerprint: "policy".to_owned(),
            }),
            ClientMessage::Subscribe {
                subscription_id: SubscriptionId::new("todos"),
                query: BuiltQuery {
                    table: "todos".to_owned(),
                    conditions: Vec::new(),
                    order_by: Vec::new(),
                    limit: Some(10),
                    offset: None,
                },
                requested_tier: SettlementTier::Local,
            },
            ClientMessage::UploadTx {
                tx: ClientTx {
                    tx_id: "018f56e2-6e2b-7d4d-9f66-4a59421e8a8f".to_owned(),
                    branch_id: None,
                    conflict_mode: TxConflictMode::Mergeable,
                    created_at: 1,
                    author: Some("alice".to_owned()),
                },
                data: vec![mini_jazz_sqlite::protocol::ClientDataRecord {
                    table: "projects".to_owned(),
                    row_id: "project-1".to_owned(),
                    op: DataOp::Insert,
                    values: BTreeMap::new(),
                }],
                reads: Vec::new(),
            },
            ClientMessage::Ack {
                message_id: mini_jazz_sqlite::protocol::MessageId(1),
                cursor: None,
            },
        ];

        let relayed = relayable_native_client_messages(&messages);

        assert_eq!(relayed.len(), 2);
        assert!(matches!(relayed[0], ClientMessage::Subscribe { .. }));
        assert!(matches!(relayed[1], ClientMessage::UploadTx { .. }));
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
            ClientHello, ClientMessage, ServerMessage, SessionId, SettlementTier, SubscriptionId,
            SUPPORTED_PROTOCOL_VERSION,
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
            native_sync: None,
        };

        let output = worker.handle_sync(RuntimeWorkerInput::Protocol {
            request_id: 7,
            client_messages: vec![
                ClientMessage::Hello(ClientHello {
                    protocol_version: SUPPORTED_PROTOCOL_VERSION,
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

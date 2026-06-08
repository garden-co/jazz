#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

use crate::browser_telemetry::BrowserTelemetryConfig;
#[cfg(target_arch = "wasm32")]
use crate::browser_telemetry::{emit_log, emit_sync_log_records};
use crate::native_sync::{
    client_sync_log_records, server_sync_log_records, NativeSyncLogContext, SyncLogRecord,
};
#[cfg(target_arch = "wasm32")]
use crate::native_sync::{
    decode_server_frame, encode_client_frame_with_context, DIRECTION_WORKER_FROM_MAIN,
    DIRECTION_WORKER_FROM_SERVER, DIRECTION_WORKER_TO_MAIN, DIRECTION_WORKER_TO_SERVER,
};
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
        native_sync_logging: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        browser_telemetry: Option<BrowserTelemetryConfig>,
    },
    ApplyBundles {
        request_id: RuntimeRequestId,
        bundles: Vec<Bundle>,
        client_messages: Vec<ClientMessage>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sync_context: Option<NativeSyncLogContext>,
    },
    Protocol {
        request_id: RuntimeRequestId,
        client_messages: Vec<ClientMessage>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sync_context: Option<NativeSyncLogContext>,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sync_context: Option<NativeSyncLogContext>,
    },
    Protocol {
        request_id: RuntimeRequestId,
        server_messages: Vec<ServerMessage>,
        storage_stats: BrowserStorageStats,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sync_context: Option<NativeSyncLogContext>,
    },
    Pushed {
        server_messages: Vec<ServerMessage>,
        storage_stats: BrowserStorageStats,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sync_context: Option<NativeSyncLogContext>,
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
    browser_telemetry: Option<BrowserTelemetryConfig>,
    sync_session_id: Option<String>,
}

struct NativeSync {
    node_id: String,
    schema_fingerprint: String,
    policy_fingerprint: String,
    logging_enabled: bool,
    server_session_id: Option<String>,
    ready: bool,
    sent_hello: bool,
    pending_client_messages: Vec<ClientMessage>,
    pending_sync_context: Option<NativeSyncLogContext>,
    browser_telemetry: Option<BrowserTelemetryConfig>,
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
            browser_telemetry: None,
            sync_session_id: None,
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
                native_sync_logging,
                browser_telemetry,
            } => {
                spawn_local(async move {
                    worker.borrow_mut().browser_telemetry = browser_telemetry.clone();
                    let records = client_sync_log_records(
                        DIRECTION_WORKER_FROM_MAIN,
                        None,
                        None,
                        &client_messages,
                    );
                    emit_sync_log_records(browser_telemetry.as_ref(), &records);
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
                                        native_sync_logging,
                                        browser_telemetry.clone(),
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
                    worker.borrow().emit_worker_output_sync_logs(&output);
                    responder.send(output);
                });
            }
            msg => responder.send(worker.borrow_mut().handle_sync(msg)),
        }
    }

    fn handle_sync(&mut self, msg: RuntimeWorkerInput) -> RuntimeWorkerOutput {
        #[cfg(target_arch = "wasm32")]
        self.emit_worker_input_sync_logs(&msg);

        match msg {
            RuntimeWorkerInput::Open { .. } => RuntimeWorkerOutput::Error {
                request_id: None,
                message: "open must be handled asynchronously".to_owned(),
            },
            RuntimeWorkerInput::ApplyBundles {
                request_id,
                bundles,
                client_messages,
                sync_context,
            } => {
                let Some(runtime) = self.runtime.as_mut() else {
                    return runtime_not_ready(request_id);
                };
                let Some(upstream_connection_manager) = self.upstream_connection_manager.as_mut()
                else {
                    return runtime_not_ready(request_id);
                };
                let native_client_messages = relayable_native_client_messages(&client_messages);
                let local_client_messages = local_worker_client_messages(&client_messages);
                let output = apply_bundles(
                    runtime,
                    upstream_connection_manager,
                    request_id,
                    bundles,
                    local_client_messages,
                    sync_context.clone(),
                );
                if let Err(message) =
                    self.send_native_client_messages(native_client_messages, sync_context.clone())
                {
                    return RuntimeWorkerOutput::Error {
                        request_id: Some(request_id),
                        message,
                    };
                }
                #[cfg(target_arch = "wasm32")]
                self.emit_worker_output_sync_logs(&output);
                output
            }
            RuntimeWorkerInput::Protocol {
                request_id,
                client_messages,
                sync_context,
            } => {
                let Some(runtime) = self.runtime.as_mut() else {
                    return runtime_not_ready(request_id);
                };
                let Some(upstream_connection_manager) = self.upstream_connection_manager.as_mut()
                else {
                    return runtime_not_ready(request_id);
                };
                let native_client_messages = relayable_native_client_messages(&client_messages);
                let local_client_messages = local_worker_client_messages(&client_messages);
                let output = protocol_messages(
                    runtime,
                    upstream_connection_manager,
                    request_id,
                    local_client_messages,
                    sync_context.clone(),
                );
                if let Err(message) =
                    self.send_native_client_messages(native_client_messages, sync_context.clone())
                {
                    return RuntimeWorkerOutput::Error {
                        request_id: Some(request_id),
                        message,
                    };
                }
                #[cfg(target_arch = "wasm32")]
                self.emit_worker_output_sync_logs(&output);
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

#[cfg(test)]
pub(crate) fn runtime_worker_input_client_log_records(
    direction: &'static str,
    input: &RuntimeWorkerInput,
) -> Vec<SyncLogRecord> {
    runtime_worker_input_client_log_records_with_session(direction, input, None)
}

pub(crate) fn runtime_worker_input_client_log_records_with_session(
    direction: &'static str,
    input: &RuntimeWorkerInput,
    fallback_session_id: Option<&str>,
) -> Vec<SyncLogRecord> {
    match input {
        RuntimeWorkerInput::Open {
            client_messages, ..
        } => {
            let sync_context = sync_context_with_fallback_session(None, fallback_session_id);
            client_sync_log_records(direction, sync_context.as_ref(), None, client_messages)
        }
        RuntimeWorkerInput::ApplyBundles {
            client_messages,
            sync_context,
            ..
        }
        | RuntimeWorkerInput::Protocol {
            client_messages,
            sync_context,
            ..
        } => {
            let sync_context =
                sync_context_with_fallback_session(sync_context.as_ref(), fallback_session_id);
            client_sync_log_records(direction, sync_context.as_ref(), None, client_messages)
        }
        RuntimeWorkerInput::ExportQuery { .. }
        | RuntimeWorkerInput::ExportQueries { .. }
        | RuntimeWorkerInput::Query { .. }
        | RuntimeWorkerInput::StorageStats { .. } => Vec::new(),
    }
}

#[cfg(test)]
pub(crate) fn runtime_worker_output_server_log_records(
    direction: &'static str,
    output: &RuntimeWorkerOutput,
) -> Vec<SyncLogRecord> {
    runtime_worker_output_server_log_records_with_session(direction, output, None)
}

pub(crate) fn runtime_worker_output_server_log_records_with_session(
    direction: &'static str,
    output: &RuntimeWorkerOutput,
    fallback_session_id: Option<&str>,
) -> Vec<SyncLogRecord> {
    match output {
        RuntimeWorkerOutput::Opened {
            server_messages, ..
        } => {
            let sync_context = sync_context_with_fallback_session(None, fallback_session_id);
            server_sync_log_records(direction, sync_context.as_ref(), None, server_messages)
        }
        RuntimeWorkerOutput::Applied {
            server_messages,
            sync_context,
            ..
        }
        | RuntimeWorkerOutput::Protocol {
            server_messages,
            sync_context,
            ..
        }
        | RuntimeWorkerOutput::Pushed {
            server_messages,
            sync_context,
            ..
        } => {
            let sync_context =
                sync_context_with_fallback_session(sync_context.as_ref(), fallback_session_id);
            server_sync_log_records(direction, sync_context.as_ref(), None, server_messages)
        }
        RuntimeWorkerOutput::Exported { .. }
        | RuntimeWorkerOutput::ExportedQueries { .. }
        | RuntimeWorkerOutput::QueryResult { .. }
        | RuntimeWorkerOutput::StorageStats { .. }
        | RuntimeWorkerOutput::Error { .. } => Vec::new(),
    }
}

pub(crate) fn runtime_worker_output_session_id(output: &RuntimeWorkerOutput) -> Option<&str> {
    match output {
        RuntimeWorkerOutput::Applied { sync_context, .. }
        | RuntimeWorkerOutput::Protocol { sync_context, .. }
        | RuntimeWorkerOutput::Pushed { sync_context, .. } => sync_context
            .as_ref()
            .and_then(|context| context.session_id.as_deref()),
        RuntimeWorkerOutput::Opened { .. }
        | RuntimeWorkerOutput::Exported { .. }
        | RuntimeWorkerOutput::ExportedQueries { .. }
        | RuntimeWorkerOutput::QueryResult { .. }
        | RuntimeWorkerOutput::StorageStats { .. }
        | RuntimeWorkerOutput::Error { .. } => None,
    }
}

fn sync_context_with_fallback_session(
    sync_context: Option<&NativeSyncLogContext>,
    fallback_session_id: Option<&str>,
) -> Option<NativeSyncLogContext> {
    let mut sync_context = sync_context.cloned();
    if sync_context
        .as_ref()
        .and_then(|context| context.session_id.as_ref())
        .is_none()
    {
        if let Some(session_id) = fallback_session_id {
            sync_context
                .get_or_insert_with(|| NativeSyncLogContext {
                    session_id: None,
                    probe: None,
                })
                .session_id = Some(session_id.to_owned());
        }
    }
    sync_context
}

impl Default for BrowserRuntimeWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl BrowserRuntimeWorker {
    #[cfg(target_arch = "wasm32")]
    fn emit_worker_input_sync_logs(&self, input: &RuntimeWorkerInput) {
        let records = runtime_worker_input_client_log_records_with_session(
            DIRECTION_WORKER_FROM_MAIN,
            input,
            self.sync_session_id.as_deref(),
        );
        emit_sync_log_records(self.browser_telemetry.as_ref(), &records);
    }

    #[cfg(target_arch = "wasm32")]
    fn emit_worker_output_sync_logs(&self, output: &RuntimeWorkerOutput) {
        let records = runtime_worker_output_server_log_records_with_session(
            DIRECTION_WORKER_TO_MAIN,
            output,
            self.sync_session_id.as_deref(),
        );
        emit_sync_log_records(self.browser_telemetry.as_ref(), &records);
    }

    fn send_native_client_messages(
        &mut self,
        client_messages: Vec<ClientMessage>,
        sync_context: Option<NativeSyncLogContext>,
    ) -> Result<(), String> {
        if client_messages.is_empty() {
            return Ok(());
        }
        let Some(sync) = self.native_sync.as_mut() else {
            return Ok(());
        };
        sync.send_or_buffer(client_messages, sync_context)
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
    sync_context: Option<NativeSyncLogContext>,
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
            sync_context,
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
    sync_context: Option<NativeSyncLogContext>,
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
                sync_context,
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
            | ClientMessage::Unsubscribe { .. }
            | ClientMessage::ReconcileSymbols { .. } => Some(message.clone()),
            ClientMessage::Hello(_) | ClientMessage::Ack { .. } | ClientMessage::Close(_) => None,
        })
        .collect()
}

fn local_worker_client_messages(client_messages: &[ClientMessage]) -> Vec<ClientMessage> {
    client_messages
        .iter()
        .filter(|message| !matches!(message, ClientMessage::ReconcileSymbols { .. }))
        .cloned()
        .collect()
}

fn apply_native_server_messages(
    runtime: &mut Runtime,
    upstream_connection_manager: &mut UpstreamConnectionManager,
    server_messages: Vec<ServerMessage>,
) -> Result<(Vec<ClientMessage>, Vec<ServerMessage>, BrowserStorageStats), String> {
    let mut client_messages = Vec::new();
    let mut forwarded_server_messages = Vec::new();
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
            ServerMessage::ReconcileMore { .. } => forwarded_server_messages.push(message),
            ServerMessage::Error(error)
                if error.subscription_id.is_some()
                    && error.retry_hint != mini_jazz_sqlite::protocol::RetryHint::Fatal =>
            {
                forwarded_server_messages.push(ServerMessage::Error(error));
            }
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
    forwarded_server_messages.extend(main_server_messages);
    let storage_stats = runtime.storage_stats().map_err(error_message)?.into();
    Ok((client_messages, forwarded_server_messages, storage_stats))
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
        logging_enabled: bool,
        browser_telemetry: Option<BrowserTelemetryConfig>,
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
                let logging_enabled = worker
                    .borrow()
                    .native_sync
                    .as_ref()
                    .is_some_and(|sync| sync.logging_enabled);
                let browser_telemetry = worker
                    .borrow()
                    .native_sync
                    .as_ref()
                    .and_then(|sync| sync.browser_telemetry.clone());
                if logging_enabled {
                    let records = server_sync_log_records(
                        DIRECTION_WORKER_FROM_SERVER,
                        frame.sync_context.as_ref(),
                        None,
                        &frame.server_messages,
                    );
                    emit_sync_log_records(browser_telemetry.as_ref(), &records);
                }
                let remote_frame_body = serde_json::json!({
                    "event": "sync.client.remote_frame_received",
                    "message_count": frame.server_messages.len(),
                })
                .to_string();
                emit_log(
                    browser_telemetry.as_ref(),
                    "sync.client.remote_frame_received",
                    frame.sync_context.as_ref(),
                    &remote_frame_body,
                    [("sync.phase", "remote_frame_received")],
                );
                let sync_context = frame.sync_context.clone();
                let output = {
                    let mut worker = worker.borrow_mut();
                    if let Some(session_id) = sync_context
                        .as_ref()
                        .and_then(|context| context.session_id.clone())
                    {
                        worker.sync_session_id = Some(session_id.clone());
                        if let Some(sync) = worker.native_sync.as_mut() {
                            sync.server_session_id = Some(session_id);
                        }
                    }
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
                            let browser_telemetry = worker
                                .native_sync
                                .as_ref()
                                .and_then(|sync| sync.browser_telemetry.clone());
                            let applied_body = serde_json::json!({
                                "event": "sync.client.remote_bundle_applied",
                                "client_message_count": client_messages.len(),
                                "main_server_message_count": main_server_messages.len(),
                            })
                            .to_string();
                            emit_log(
                                browser_telemetry.as_ref(),
                                "sync.client.remote_bundle_applied",
                                sync_context.as_ref(),
                                &applied_body,
                                [("sync.phase", "remote_bundle_applied")],
                            );
                            if let Some(sync) = worker.native_sync.as_mut() {
                                if let Err(message) = sync.send_or_buffer(client_messages, None) {
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
                                sync_context,
                            }
                        }
                        Err(message) => RuntimeWorkerOutput::Error {
                            request_id: None,
                            message,
                        },
                    }
                };
                worker.borrow().emit_worker_output_sync_logs(&output);
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
            logging_enabled,
            server_session_id: None,
            ready: false,
            sent_hello: false,
            pending_client_messages: Vec::new(),
            pending_sync_context: None,
            browser_telemetry,
            socket,
            _onopen: onopen,
            _onmessage: onmessage,
            _onerror: onerror,
            _onclose: onclose,
        })
    }

    fn send_or_buffer(
        &mut self,
        client_messages: Vec<ClientMessage>,
        sync_context: Option<NativeSyncLogContext>,
    ) -> Result<(), String> {
        if client_messages.is_empty() {
            return Ok(());
        }
        self.pending_client_messages.extend(client_messages);
        if sync_context.is_some() {
            self.pending_sync_context = sync_context;
        }
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
            let sync_context = self
                .pending_sync_context
                .take()
                .or_else(|| self.next_sync_context());
            let sync_context = self.with_server_session(sync_context);
            if self.logging_enabled {
                let records = client_sync_log_records(
                    DIRECTION_WORKER_TO_SERVER,
                    sync_context.as_ref(),
                    None,
                    &client_messages,
                );
                emit_sync_log_records(self.browser_telemetry.as_ref(), &records);
            }
            let sent_body = serde_json::json!({
                "event": "sync.client.frame_sent",
                "message_count": client_messages.len(),
            })
            .to_string();
            emit_log(
                self.browser_telemetry.as_ref(),
                "sync.client.frame_sent",
                sync_context.as_ref(),
                &sent_body,
                [("sync.phase", "client_frame_sent")],
            );
            let encoded = encode_client_frame_with_context(client_messages, sync_context)?;
            self.socket
                .send_with_str(&encoded)
                .map_err(|error| format!("send native sync frame: {error:?}"))?;
        }
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    fn next_sync_context(&mut self) -> Option<NativeSyncLogContext> {
        if !self.logging_enabled {
            return None;
        }
        Some(NativeSyncLogContext {
            session_id: self.server_session_id.clone(),
            probe: None,
        })
    }

    #[cfg(target_arch = "wasm32")]
    fn with_server_session(
        &self,
        mut sync_context: Option<NativeSyncLogContext>,
    ) -> Option<NativeSyncLogContext> {
        if let Some(context) = sync_context.as_mut() {
            if context.session_id.is_none() {
                context.session_id = self.server_session_id.clone();
            }
        }
        sync_context
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
            native_sync_logging: true,
            browser_telemetry: None,
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
                    reconciliation: Some(empty_reconciliation()),
                }],
            }],
            sync_context: None,
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
            sync_context: None,
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
            sync_context: None,
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
            sync_context: None,
        };
        let decoded: RuntimeWorkerOutput = serde_round_trip(&output);

        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(output).unwrap()
        );
    }

    #[test]
    fn worker_input_sync_log_records_track_worker_from_main_messages() {
        let message = RuntimeWorkerInput::Protocol {
            request_id: 99,
            client_messages: vec![mini_jazz_sqlite::protocol::ClientMessage::Close(
                mini_jazz_sqlite::protocol::CloseReason::ClientClosed,
            )],
            sync_context: Some(NativeSyncLogContext {
                session_id: Some("server-session-1".to_owned()),
                probe: None,
            }),
        };

        let records = runtime_worker_input_client_log_records(
            crate::native_sync::DIRECTION_WORKER_FROM_MAIN,
            &message,
        );

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].attribute("sync.direction"),
            Some("worker.from_main")
        );
        assert_eq!(
            records[0].attribute("sync.session_id"),
            Some("server-session-1")
        );
        assert_eq!(
            records[0].attribute("sync.message_kind"),
            Some("client.close")
        );
        assert!(records[0].body.contains("sync.message"));
    }

    #[test]
    fn worker_output_sync_log_records_track_worker_to_main_messages() {
        let output = RuntimeWorkerOutput::Protocol {
            request_id: 99,
            server_messages: vec![mini_jazz_sqlite::protocol::ServerMessage::Close(
                mini_jazz_sqlite::protocol::CloseReason::ClientClosed,
            )],
            storage_stats: BrowserStorageStats::default(),
            sync_context: Some(NativeSyncLogContext {
                session_id: Some("server-session-1".to_owned()),
                probe: None,
            }),
        };

        let records = runtime_worker_output_server_log_records(
            crate::native_sync::DIRECTION_WORKER_TO_MAIN,
            &output,
        );

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].attribute("sync.direction"),
            Some("worker.to_main")
        );
        assert_eq!(
            records[0].attribute("sync.session_id"),
            Some("server-session-1")
        );
        assert_eq!(
            records[0].attribute("sync.message_kind"),
            Some("server.close")
        );
        assert!(records[0].body.contains("sync.message"));
    }

    #[test]
    fn worker_boundary_sync_log_records_use_fallback_session_id() {
        let input = RuntimeWorkerInput::Protocol {
            request_id: 99,
            client_messages: vec![mini_jazz_sqlite::protocol::ClientMessage::Close(
                mini_jazz_sqlite::protocol::CloseReason::ClientClosed,
            )],
            sync_context: Some(NativeSyncLogContext {
                session_id: None,
                probe: Some(crate::native_sync::NativeSyncProbe {
                    probe_id: "probe-1".to_owned(),
                    operation: "insert".to_owned(),
                    table: "todos".to_owned(),
                    row_id: "todo-1".to_owned(),
                    origin_browser_id: "browser-a".to_owned(),
                }),
            }),
        };

        let records = runtime_worker_input_client_log_records_with_session(
            crate::native_sync::DIRECTION_MAIN_TO_WORKER,
            &input,
            Some("server-session-1"),
        );

        assert_eq!(
            records[0].attribute("sync.session_id"),
            Some("server-session-1")
        );
        assert_eq!(records[0].attribute("sync.probe.id"), Some("probe-1"));

        let output = RuntimeWorkerOutput::Protocol {
            request_id: 99,
            server_messages: vec![mini_jazz_sqlite::protocol::ServerMessage::Close(
                mini_jazz_sqlite::protocol::CloseReason::ClientClosed,
            )],
            storage_stats: BrowserStorageStats::default(),
            sync_context: None,
        };
        let records = runtime_worker_output_server_log_records_with_session(
            crate::native_sync::DIRECTION_MAIN_FROM_WORKER,
            &output,
            Some("server-session-1"),
        );

        assert_eq!(
            records[0].attribute("sync.session_id"),
            Some("server-session-1")
        );
    }

    #[test]
    fn worker_relays_subscriptions_and_uploads_to_native_sync() {
        use mini_jazz_sqlite::protocol::{
            ClientMessage, ClientTx, DataOp, ReconcileParameters, ReconcileSet, SettlementTier,
            SubscriptionId, TxConflictMode,
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
                reconciliation: Some(empty_reconciliation()),
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
            ClientMessage::ReconcileSymbols {
                subscription_id: SubscriptionId::new("todos"),
                set: ReconcileSet::RowHeads,
                parameters: ReconcileParameters {
                    seed: 7,
                    estimated_items: 0,
                    target_degree: 3,
                    symbol_count: 4,
                },
                symbols: Vec::new(),
            },
        ];

        let relayed = relayable_native_client_messages(&messages);

        assert_eq!(relayed.len(), 3);
        assert!(matches!(relayed[0], ClientMessage::Subscribe { .. }));
        assert!(matches!(relayed[1], ClientMessage::UploadTx { .. }));
        assert!(matches!(relayed[2], ClientMessage::ReconcileSymbols { .. }));
    }

    #[test]
    fn worker_keeps_reconcile_symbols_out_of_local_protocol() {
        use mini_jazz_sqlite::protocol::{
            ClientMessage, ReconcileParameters, ReconcileSet, ServerMessage, SubscriptionId,
        };

        let runtime =
            Runtime::open_with_schema(Storage::Memory, "worker", "alice", todo_schema()).unwrap();
        let schema_fingerprint = runtime.local_schema_fingerprint();
        let policy_fingerprint = runtime.local_policy_fingerprint();
        let mut worker = BrowserRuntimeWorker {
            runtime: Some(runtime),
            upstream_connection_manager: Some(UpstreamConnectionManager::new(
                "worker-session",
                "worker",
                schema_fingerprint,
                policy_fingerprint,
            )),
            native_sync: None,
            browser_telemetry: None,
            sync_session_id: None,
        };

        let output = worker.handle_sync(RuntimeWorkerInput::Protocol {
            request_id: 7,
            client_messages: vec![ClientMessage::ReconcileSymbols {
                subscription_id: SubscriptionId::new("todos"),
                set: ReconcileSet::RowHeads,
                parameters: ReconcileParameters {
                    seed: 7,
                    estimated_items: 0,
                    target_degree: 3,
                    symbol_count: 4,
                },
                symbols: Vec::new(),
            }],
            sync_context: None,
        });

        let RuntimeWorkerOutput::Protocol {
            server_messages, ..
        } = output
        else {
            panic!("expected symbols to be ignored by the local worker protocol");
        };
        assert!(
            server_messages
                .iter()
                .all(|message| !matches!(message, ServerMessage::Error(_))),
            "local worker protocol should not emit reconciliation errors"
        );
    }

    #[test]
    fn worker_forwards_retryable_scoped_native_errors_to_main() {
        use mini_jazz_sqlite::protocol::{
            ClientHello, ClientMessage, ProtocolError, RetryHint, ServerMessage, SessionId,
            SubscriptionId, SUPPORTED_PROTOCOL_VERSION,
        };

        let mut runtime =
            Runtime::open_with_schema(Storage::Memory, "worker", "alice", todo_schema()).unwrap();
        let schema_fingerprint = runtime.local_schema_fingerprint();
        let policy_fingerprint = runtime.local_policy_fingerprint();
        let mut upstream_connection_manager = UpstreamConnectionManager::new(
            "worker-session",
            "worker",
            schema_fingerprint.clone(),
            policy_fingerprint.clone(),
        );
        upstream_connection_manager
            .receive(
                &mut runtime,
                vec![ClientMessage::Hello(ClientHello {
                    protocol_version: SUPPORTED_PROTOCOL_VERSION,
                    session_id: SessionId::new("main-session"),
                    node_id: "main".to_owned(),
                    schema_fingerprint,
                    policy_fingerprint,
                })],
            )
            .unwrap();
        let subscription_id = SubscriptionId::new("downstream-subscription-0");

        let (_client_messages, forwarded_server_messages, _storage_stats) =
            apply_native_server_messages(
                &mut runtime,
                &mut upstream_connection_manager,
                vec![ServerMessage::Error(ProtocolError {
                    code: "reconciliation_decode_failed".to_owned(),
                    message: "rateless reconciliation could not be decoded".to_owned(),
                    subscription_id: Some(subscription_id.clone()),
                    message_id: None,
                    retry_hint: RetryHint::Retryable,
                })],
            )
            .unwrap();

        assert!(matches!(
            &forwarded_server_messages[..],
            [ServerMessage::Error(error)]
                if error.code == "reconciliation_decode_failed"
                    && error.subscription_id.as_ref() == Some(&subscription_id)
        ));
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
            browser_telemetry: None,
            sync_session_id: None,
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
                    reconciliation: Some(empty_reconciliation()),
                },
            ],
            sync_context: None,
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

    fn empty_reconciliation() -> mini_jazz_sqlite::protocol::ReconciliationSketch {
        mini_jazz_sqlite::protocol::ReconciliationSketch {
            set: mini_jazz_sqlite::protocol::ReconcileSet::RowHeads,
            algorithm: mini_jazz_sqlite::protocol::ReconcileAlgorithm::Exact,
            parameters: None,
            symbols: Vec::new(),
            row_heads: Vec::new(),
        }
    }
}

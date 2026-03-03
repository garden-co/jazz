// jazz-rn (Rust) — UniFFI surface for React Native.
//
// Note: This crate intentionally uses UniFFI proc-macros (no UDL). The RN bindings
// generator runs UniFFI in "library mode", reading this crate's metadata.
uniffi::setup_scaffolding!();

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::executor::block_on;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::encoding::decode_row;
use jazz_tools::query_manager::manager::LocalUpdates;
use jazz_tools::query_manager::parse_query_json;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{RowDescriptor, Schema, SchemaHash, TableName, Value};
use jazz_tools::runtime_core::{
    ReadDurabilityOptions, RuntimeCore, Scheduler, SubscriptionDelta, SubscriptionHandle,
    SyncSender,
};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::SurrealKvStorage;
use jazz_tools::sync_manager::{
    ClientId, Destination, DurabilityTier, InboxEntry, OutboxEntry, QueryPropagation, ServerId,
    Source, SyncManager, SyncPayload,
};

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum JazzRnError {
    #[error("invalid json: {message}")]
    InvalidJson { message: String },

    #[error("invalid uuid: {message}")]
    InvalidUuid { message: String },

    #[error("invalid persistence tier: {message}")]
    InvalidTier { message: String },

    #[error("schema error: {message}")]
    Schema { message: String },

    #[error("runtime error: {message}")]
    Runtime { message: String },

    #[error("internal error: {message}")]
    Internal { message: String },
}

fn json_err(e: serde_json::Error) -> JazzRnError {
    JazzRnError::InvalidJson {
        message: e.to_string(),
    }
}

fn runtime_err<E: std::fmt::Debug>(e: E) -> JazzRnError {
    JazzRnError::Runtime {
        message: format!("{:?}", e),
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    "non-string panic payload".to_string()
}

fn with_panic_boundary<T, F>(context: &'static str, f: F) -> Result<T, JazzRnError>
where
    F: FnOnce() -> Result<T, JazzRnError>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let panic_message = panic_payload_to_string(payload);
            let backtrace = std::backtrace::Backtrace::force_capture();
            Err(JazzRnError::Internal {
                message: format!("panic in {context}: {panic_message}\n{backtrace}"),
            })
        }
    }
}

fn convert_values(values_json: &str) -> Result<Vec<Value>, JazzRnError> {
    serde_json::from_str(values_json).map_err(json_err)
}

fn convert_updates(values_json: &str) -> Result<Vec<(String, Value)>, JazzRnError> {
    let partial: HashMap<String, Value> = serde_json::from_str(values_json).map_err(json_err)?;
    Ok(partial.into_iter().collect())
}

fn reorder_values_by_column_name(
    source_descriptor: &RowDescriptor,
    target_descriptor: &RowDescriptor,
    values: &[Value],
) -> Option<Vec<Value>> {
    if values.len() != source_descriptor.columns.len()
        || source_descriptor.columns.len() != target_descriptor.columns.len()
    {
        return None;
    }

    let mut values_by_column = HashMap::with_capacity(values.len());
    for (column, value) in source_descriptor.columns.iter().zip(values.iter()) {
        values_by_column.insert(column.name, value.clone());
    }

    let mut reordered_values = Vec::with_capacity(values.len());
    for column in &target_descriptor.columns {
        reordered_values.push(values_by_column.remove(&column.name)?);
    }

    Some(reordered_values)
}

fn align_row_values_to_declared_schema(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    table: &TableName,
    values: Vec<Value>,
) -> Vec<Value> {
    let Some(declared_table) = declared_schema.get(table) else {
        return values;
    };
    let Some(runtime_table) = runtime_schema.get(table) else {
        return values;
    };

    reorder_values_by_column_name(&runtime_table.columns, &declared_table.columns, &values)
        .unwrap_or(values)
}

fn parse_query(query_json: &str) -> Result<Query, JazzRnError> {
    parse_query_json(query_json).map_err(|message| JazzRnError::InvalidJson { message })
}

fn parse_session(session_json: Option<String>) -> Result<Option<Session>, JazzRnError> {
    match session_json {
        Some(json) => Ok(Some(serde_json::from_str(&json).map_err(json_err)?)),
        None => Ok(None),
    }
}

fn parse_tier(tier: &str) -> Result<DurabilityTier, JazzRnError> {
    match tier {
        "worker" => Ok(DurabilityTier::Worker),
        "edge" => Ok(DurabilityTier::EdgeServer),
        "global" => Ok(DurabilityTier::GlobalServer),
        _ => Err(JazzRnError::InvalidTier {
            message: format!(
                "Invalid tier '{}'. Must be 'worker', 'edge', or 'global'.",
                tier
            ),
        }),
    }
}

fn default_read_durability_options(tier: Option<DurabilityTier>) -> ReadDurabilityOptions {
    ReadDurabilityOptions {
        tier,
        local_updates: LocalUpdates::Immediate,
    }
}

fn parse_subscription_inputs(
    query_json: &str,
    session_json: Option<String>,
    tier: Option<String>,
) -> Result<(Query, Option<Session>, ReadDurabilityOptions), JazzRnError> {
    let query = parse_query(query_json)?;
    let session = parse_session(session_json)?;
    let tier = tier.as_deref().map(parse_tier).transpose()?;
    Ok((query, session, default_read_durability_options(tier)))
}

fn build_rn_delta_json<F>(delta: &SubscriptionDelta, mut row_to_json: F) -> serde_json::Value
where
    F: FnMut(&jazz_tools::query_manager::types::Row) -> serde_json::Value,
{
    let removed = delta
        .ordered_delta
        .removed
        .iter()
        .map(|change| {
            serde_json::json!({
                "kind": 1,
                "id": change.id.uuid().to_string(),
                "index": change.index
            })
        })
        .collect::<Vec<_>>();

    let updated = delta
        .ordered_delta
        .updated
        .iter()
        .map(|change| {
            serde_json::json!({
                "kind": 2,
                "id": change.id.uuid().to_string(),
                "index": change.new_index,
                "row": change.row.as_ref().map(&mut row_to_json)
            })
        })
        .collect::<Vec<_>>();

    let added = delta
        .ordered_delta
        .added
        .iter()
        .map(|change| {
            let row_json = row_to_json(&change.row);
            serde_json::json!({
                "kind": 0,
                "id": change.id.uuid().to_string(),
                "index": change.index,
                "row": row_json
            })
        })
        .collect::<Vec<_>>();

    let changes = removed
        .into_iter()
        .chain(updated)
        .chain(added)
        .collect::<Vec<_>>();

    serde_json::Value::Array(changes)
}

fn subscription_delta_to_json(delta: &SubscriptionDelta) -> serde_json::Value {
    let descriptor = &delta.descriptor;
    let row_to_json = |row: &jazz_tools::query_manager::types::Row| -> serde_json::Value {
        let values = decode_row(descriptor, &row.data)
            .map(|vals| vals.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        serde_json::json!({
            "id": row.id.uuid().to_string(),
            "values": values,
        })
    };
    build_rn_delta_json(delta, row_to_json)
}

fn make_subscription_callback(
    callback: Box<dyn SubscriptionCallback>,
) -> impl Fn(SubscriptionDelta) + Send + 'static {
    move |delta: SubscriptionDelta| {
        let payload = subscription_delta_to_json(&delta);
        if let Ok(json) = serde_json::to_string(&payload) {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                callback.on_update(json);
            }));
        }
    }
}

// ============================================================================
// Callbacks (JS-implemented) for scheduling + sync output
// ============================================================================

#[uniffi::export(callback_interface)]
pub trait BatchedTickCallback: Send + Sync {
    /// Called by Rust when it wants JS to call `runtime.batched_tick()`.
    fn request_batched_tick(&self);
}

#[uniffi::export(callback_interface)]
pub trait SyncMessageCallback: Send + Sync {
    /// Called by Rust when it has an outbox message to send.
    fn on_sync_message(
        &self,
        destination_kind: String,
        destination_id: String,
        payload_json: String,
        is_catalogue: bool,
    );
}

#[uniffi::export(callback_interface)]
pub trait SubscriptionCallback: Send + Sync {
    /// Called when a subscription produces an update.
    fn on_update(&self, delta_json: String);
}

// ============================================================================
// RnScheduler + RnSyncSender
// ============================================================================

#[derive(Clone, Default)]
struct RnScheduler {
    scheduled: Arc<AtomicBool>,
    callback: Arc<Mutex<Option<Box<dyn BatchedTickCallback>>>>,
}

impl RnScheduler {
    fn set_callback(&self, cb: Option<Box<dyn BatchedTickCallback>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = cb;
        }
    }

    fn clear_scheduled(&self) {
        self.scheduled.store(false, Ordering::SeqCst);
    }
}

impl Scheduler for RnScheduler {
    fn schedule_batched_tick(&self) {
        // Debounce: only one pending tick request at a time.
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            let called = if let Ok(guard) = self.callback.lock() {
                if let Some(cb) = guard.as_ref() {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        cb.request_batched_tick();
                    }))
                    .is_ok()
                } else {
                    false
                }
            } else {
                false
            };

            // No callback registered; allow future scheduling attempts.
            if !called {
                self.scheduled.store(false, Ordering::SeqCst);
            }
        }
    }
}

#[derive(Clone, Default)]
struct RnSyncSender {
    callback: Arc<Mutex<Option<Box<dyn SyncMessageCallback>>>>,
}

impl RnSyncSender {
    fn set_callback(&self, cb: Option<Box<dyn SyncMessageCallback>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = cb;
        }
    }
}

impl SyncSender for RnSyncSender {
    fn send_sync_message(&self, message: OutboxEntry,  _sender_tier: &'static str) {
        let is_catalogue = message.payload.is_catalogue();
        let Ok(payload_json) = serde_json::to_string(&message.payload) else {
            return;
        };
        let (destination_kind, destination_id) = match message.destination {
            Destination::Server(server_id) => ("server".to_string(), server_id.0.to_string()),
            Destination::Client(client_id) => ("client".to_string(), client_id.0.to_string()),
        };

        if let Ok(guard) = self.callback.lock() {
            if let Some(cb) = guard.as_ref() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    cb.on_sync_message(
                        destination_kind,
                        destination_id,
                        payload_json,
                        is_catalogue,
                    );
                }));
            }
        }
    }
}

// ============================================================================
// RnRuntime
// ============================================================================

type RnCoreType = RuntimeCore<SurrealKvStorage, RnScheduler, RnSyncSender>;

#[derive(uniffi::Object)]
pub struct RnRuntime {
    core: Mutex<RnCoreType>,
    upstream_server_id: Mutex<Option<ServerId>>,
    declared_schema: Schema,
}

#[uniffi::export]
impl RnRuntime {
    #[uniffi::constructor]
    pub fn new(
        schema_json: String,
        app_id: String,
        jazz_env: String,
        user_branch: String,
        tier: Option<String>,
        data_path: Option<String>,
    ) -> Result<Arc<Self>, JazzRnError> {
        with_panic_boundary("new", || {
            let schema: Schema = serde_json::from_str(&schema_json).map_err(json_err)?;
            let declared_schema = schema.clone();

            let persistence_tier = tier.as_deref().map(parse_tier).transpose()?;

            let mut sync_manager = SyncManager::new();
            if let Some(t) = persistence_tier {
                sync_manager = sync_manager.with_durability_tier(t);
            }

            let app_id_obj =
                AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id));
            let schema_manager =
                SchemaManager::new(sync_manager, schema, app_id_obj, &jazz_env, &user_branch)
                    .map_err(|e| JazzRnError::Schema {
                        message: format!("{:?}", e),
                    })?;

            let cache_size_bytes = 64 * 1024 * 1024; // 64MB
            let resolved_data_path = data_path.unwrap_or_else(|| {
                let sanitized_app_id: String = app_id
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect();
                let mut default_path = std::env::temp_dir();
                default_path.push(format!("{sanitized_app_id}.surrealkv"));
                default_path.to_string_lossy().into_owned()
            });
            let storage =
                SurrealKvStorage::open(&resolved_data_path, cache_size_bytes).map_err(|e| {
                    JazzRnError::Runtime {
                        message: format!(
                            "Failed to open SurrealKV storage at '{}': {:?}",
                            resolved_data_path, e
                        ),
                    }
                })?;
            let scheduler = RnScheduler::default();
            let sync_sender = RnSyncSender::default();

            let mut core = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);
            core.persist_schema();

            Ok(Arc::new(Self {
                core: Mutex::new(core),
                upstream_server_id: Mutex::new(None),
                declared_schema,
            }))
        })
    }

    /// Register a JS callback that schedules `batched_tick()` calls.
    pub fn on_batched_tick_needed(
        &self,
        callback: Option<Box<dyn BatchedTickCallback>>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("on_batched_tick_needed", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.scheduler_mut().set_callback(callback);
            Ok(())
        })
    }

    /// Register a JS callback for outbound sync messages.
    pub fn on_sync_message_to_send(
        &self,
        callback: Option<Box<dyn SyncMessageCallback>>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("on_sync_message_to_send", || {
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.sync_sender().set_callback(callback);
            Ok(())
        })
    }

    /// Run a batched tick. JS should call this when asked via `on_batched_tick_needed`.
    pub fn batched_tick(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("batched_tick", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.scheduler_mut().clear_scheduled();
            core.batched_tick();
            Ok(())
        })
    }

    // =========================================================================
    // CRUD
    // =========================================================================

    pub fn insert(&self, table: String, values_json: String) -> Result<String, JazzRnError> {
        with_panic_boundary("insert", || {
            let values = convert_values(&values_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let (id, row_values) = core.insert(&table, values, None).map_err(runtime_err)?;
            let row_values = align_row_values_to_declared_schema(
                &self.declared_schema,
                core.current_schema(),
                &TableName::new(table.clone()),
                row_values,
            );
            serde_json::to_string(&serde_json::json!({
                "id": id.uuid().to_string(),
                "values": row_values,
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("insert serialization failed: {e}"),
            })
        })
    }

    pub fn update(&self, object_id: String, values_json: String) -> Result<(), JazzRnError> {
        with_panic_boundary("update", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let updates = convert_updates(&values_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.update(oid, updates, None).map_err(runtime_err)?;
            Ok(())
        })
    }

    #[uniffi::method(name = "delete")]
    pub fn delete_row(&self, object_id: String) -> Result<(), JazzRnError> {
        with_panic_boundary("delete", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.delete(oid, None).map_err(runtime_err)?;
            Ok(())
        })
    }

    // =========================================================================
    // Queries
    // =========================================================================

    /// One-shot query returning a JSON string:
    /// `[{ "id": "<uuid>", "values": [ {type, value}, ... ] }, ...]`.
    pub fn query(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("query", || {
            let query = parse_query(&query_json)?;
            let session = parse_session(session_json)?;
            let tier = tier.as_deref().map(parse_tier).transpose()?;

            // NOTE: query() triggers immediate_tick() internally.
            // We then block for the first callback result to be delivered.
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let fut = core.query_with_propagation(
                query,
                session,
                ReadDurabilityOptions {
                    tier,
                    local_updates: LocalUpdates::Immediate,
                },
                QueryPropagation::Full,
            );
            let results = block_on(fut).map_err(runtime_err)?;

            let rows_json: Vec<serde_json::Value> = results
                .into_iter()
                .map(|(id, values)| {
                    serde_json::json!({
                        "id": id.uuid().to_string(),
                        "values": values,
                    })
                })
                .collect();

            serde_json::to_string(&rows_json).map_err(json_err)
        })
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    pub fn subscribe(
        &self,
        query_json: String,
        callback: Box<dyn SubscriptionCallback>,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<u64, JazzRnError> {
        with_panic_boundary("subscribe", || {
            let (query, session, durability) =
                parse_subscription_inputs(&query_json, session_json, tier)?;
            let callback = make_subscription_callback(callback);

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;

            let handle = core
                .subscribe_with_durability_and_propagation(
                    query,
                    callback,
                    session,
                    durability,
                    QueryPropagation::Full,
                )
                .map_err(runtime_err)?;

            Ok(handle.0)
        })
    }

    pub fn unsubscribe(&self, handle: u64) -> Result<(), JazzRnError> {
        with_panic_boundary("unsubscribe", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.unsubscribe(SubscriptionHandle(handle));
            Ok(())
        })
    }

    /// Phase 1 of 2-phase subscribe: allocate a handle and store query params.
    pub fn create_subscription(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<u64, JazzRnError> {
        with_panic_boundary("create_subscription", || {
            let (query, session, durability) =
                parse_subscription_inputs(&query_json, session_json, tier)?;

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;

            let handle =
                core.create_subscription(query, session, durability, QueryPropagation::Full);

            Ok(handle.0)
        })
    }

    /// Phase 2 of 2-phase subscribe: compile, register, sync, attach callback, tick.
    pub fn execute_subscription(
        &self,
        handle: u64,
        callback: Box<dyn SubscriptionCallback>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("execute_subscription", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let callback = make_subscription_callback(callback);

            core.execute_subscription(SubscriptionHandle(handle), callback)
                .map_err(runtime_err)?;

            Ok(())
        })
    }

    // =========================================================================
    // Sync
    // =========================================================================

    pub fn on_sync_message_received(&self, message_json: String) -> Result<(), JazzRnError> {
        with_panic_boundary("on_sync_message_received", || {
            let payload: SyncPayload = serde_json::from_str(&message_json).map_err(json_err)?;
            let entry = InboxEntry {
                source: Source::Server(ServerId::new()),
                payload,
            };
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.park_sync_message(entry);
            Ok(())
        })
    }

    pub fn on_sync_message_received_from_client(
        &self,
        client_id: String,
        message_json: String,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("on_sync_message_received_from_client", || {
            let uuid = uuid::Uuid::parse_str(&client_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let cid = ClientId(uuid);
            let payload: SyncPayload = serde_json::from_str(&message_json).map_err(json_err)?;

            let entry = InboxEntry {
                source: Source::Client(cid),
                payload,
            };
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.park_sync_message(entry);
            Ok(())
        })
    }

    pub fn add_server(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("add_server", || {
            let server_id = {
                let mut slot =
                    self.upstream_server_id
                        .lock()
                        .map_err(|_| JazzRnError::Internal {
                            message: "lock poisoned".into(),
                        })?;
                if let Some(server_id) = *slot {
                    server_id
                } else {
                    let server_id = ServerId::new();
                    *slot = Some(server_id);
                    server_id
                }
            };

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.remove_server(server_id);
            core.add_server(server_id);
            Ok(())
        })
    }

    pub fn remove_server(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("remove_server", || {
            let server_id = {
                let slot = self
                    .upstream_server_id
                    .lock()
                    .map_err(|_| JazzRnError::Internal {
                        message: "lock poisoned".into(),
                    })?;
                *slot
            };
            let Some(server_id) = server_id else {
                return Ok(());
            };
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.remove_server(server_id);
            Ok(())
        })
    }

    pub fn add_client(&self) -> Result<String, JazzRnError> {
        with_panic_boundary("add_client", || {
            let client_id = ClientId::new();
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.add_client(client_id, None);
            Ok(client_id.0.to_string())
        })
    }

    pub fn set_client_role(&self, client_id: String, role: String) -> Result<(), JazzRnError> {
        with_panic_boundary("set_client_role", || {
            use jazz_tools::sync_manager::ClientRole;

            let uuid = uuid::Uuid::parse_str(&client_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let cid = ClientId(uuid);

            let client_role = match role.as_str() {
                "user" => ClientRole::User,
                "admin" => ClientRole::Admin,
                "peer" => ClientRole::Peer,
                _ => {
                    return Err(JazzRnError::Runtime {
                        message: format!(
                            "Invalid role '{}'. Must be 'user', 'admin', or 'peer'.",
                            role
                        ),
                    });
                }
            };

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.set_client_role_by_name(cid, client_role);
            Ok(())
        })
    }

    // =========================================================================
    // Schema/state access
    // =========================================================================

    pub fn get_schema_hash(&self) -> Result<String, JazzRnError> {
        with_panic_boundary("get_schema_hash", || {
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let schema = core.current_schema();
            Ok(SchemaHash::compute(schema).to_string())
        })
    }

    pub fn flush(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("flush", || {
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.flush_storage();
            Ok(())
        })
    }

    /// Flush and close the underlying storage, releasing filesystem locks.
    pub fn close(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("close", || {
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.flush_storage();
            core.storage().close().map_err(runtime_err)?;
            Ok(())
        })
    }
}

// ============================================================================
// Module-level utilities
// ============================================================================

#[uniffi::export]
pub fn generate_id() -> String {
    ObjectId::new().uuid().to_string()
}

#[uniffi::export]
pub fn current_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

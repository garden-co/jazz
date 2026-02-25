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
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{Schema, SchemaHash, Value};
use jazz_tools::runtime_core::{
    RuntimeCore, Scheduler, SubscriptionDelta, SubscriptionHandle, SyncSender,
};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::SurrealKvStorage;
use jazz_tools::sync_manager::{
    ClientId, InboxEntry, OutboxEntry, PersistenceTier, ServerId, Source, SyncManager, SyncPayload,
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

// ============================================================================
// JSON boundary types (mirrors jazz-napi + jazz-wasm)
// ============================================================================

/// Tagged value type for the JS boundary, serde-serialized as:
/// `{ "type": "Text", "value": "..." }`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "value")]
enum RnValue {
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(String),
    Array(Vec<RnValue>),
    Row(Vec<RnValue>),
    Null,
}

impl From<Value> for RnValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Integer(i) => RnValue::Integer(i),
            Value::BigInt(i) => RnValue::BigInt(i),
            Value::Boolean(b) => RnValue::Boolean(b),
            Value::Text(s) => RnValue::Text(s),
            Value::Timestamp(t) => RnValue::Timestamp(t),
            Value::Uuid(id) => RnValue::Uuid(id.uuid().to_string()),
            Value::Array(arr) => RnValue::Array(arr.into_iter().map(Into::into).collect()),
            Value::Row(row) => RnValue::Row(row.into_iter().map(Into::into).collect()),
            Value::Null => RnValue::Null,
        }
    }
}

impl TryFrom<RnValue> for Value {
    type Error = JazzRnError;

    fn try_from(v: RnValue) -> Result<Self, Self::Error> {
        Ok(match v {
            RnValue::Integer(i) => Value::Integer(i),
            RnValue::BigInt(i) => Value::BigInt(i),
            RnValue::Boolean(b) => Value::Boolean(b),
            RnValue::Text(s) => Value::Text(s),
            RnValue::Timestamp(t) => Value::Timestamp(t),
            RnValue::Uuid(s) => {
                let uuid = uuid::Uuid::parse_str(&s).map_err(|e| JazzRnError::InvalidUuid {
                    message: e.to_string(),
                })?;
                Value::Uuid(ObjectId::from_uuid(uuid))
            }
            RnValue::Array(arr) => {
                let converted = arr
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<Value>, _>>()?;
                Value::Array(converted)
            }
            RnValue::Row(row) => {
                let converted = row
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<Value>, _>>()?;
                Value::Row(converted)
            }
            RnValue::Null => Value::Null,
        })
    }
}

fn convert_values(values_json: &str) -> Result<Vec<Value>, JazzRnError> {
    let js_values: Vec<RnValue> = serde_json::from_str(values_json).map_err(json_err)?;
    js_values.into_iter().map(TryInto::try_into).collect()
}

fn convert_updates(values_json: &str) -> Result<Vec<(String, Value)>, JazzRnError> {
    let partial: HashMap<String, RnValue> = serde_json::from_str(values_json).map_err(json_err)?;
    partial
        .into_iter()
        .map(|(k, v)| Ok((k, v.try_into()?)))
        .collect()
}

// ============================================================================
// Schema types for JSON deserialization
// ============================================================================

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsColumnType {
    #[serde(rename = "type")]
    type_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    element: Option<Box<JsColumnType>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<JsColumnDescriptor>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsColumnDescriptor {
    name: String,
    column_type: JsColumnType,
    nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    references: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsTableSchema {
    columns: Vec<JsColumnDescriptor>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsSchema {
    tables: HashMap<String, JsTableSchema>,
}

impl TryFrom<JsColumnType> for jazz_tools::query_manager::types::ColumnType {
    type Error = JazzRnError;

    fn try_from(ct: JsColumnType) -> Result<Self, Self::Error> {
        use jazz_tools::query_manager::types::{ColumnType, RowDescriptor};

        match ct.type_name.as_str() {
            "Integer" => Ok(ColumnType::Integer),
            "BigInt" => Ok(ColumnType::BigInt),
            "Boolean" => Ok(ColumnType::Boolean),
            "Text" => Ok(ColumnType::Text),
            "Timestamp" => Ok(ColumnType::Timestamp),
            "Uuid" => Ok(ColumnType::Uuid),
            "Array" => {
                let elem = ct.element.ok_or_else(|| JazzRnError::Schema {
                    message: "Array type requires element".to_string(),
                })?;
                let element: ColumnType = (*elem).try_into()?;
                Ok(ColumnType::Array(Box::new(element)))
            }
            "Row" => {
                let cols = ct.columns.ok_or_else(|| JazzRnError::Schema {
                    message: "Row type requires columns".to_string(),
                })?;
                let descriptors = cols
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<jazz_tools::query_manager::types::ColumnDescriptor>, _>>(
                    )?;
                Ok(ColumnType::Row(Box::new(RowDescriptor::new(descriptors))))
            }
            other => Err(JazzRnError::Schema {
                message: format!("Unknown column type: {other}"),
            }),
        }
    }
}

impl TryFrom<JsColumnDescriptor> for jazz_tools::query_manager::types::ColumnDescriptor {
    type Error = JazzRnError;

    fn try_from(c: JsColumnDescriptor) -> Result<Self, Self::Error> {
        use jazz_tools::query_manager::types::ColumnDescriptor;

        let mut cd = ColumnDescriptor::new(&c.name, c.column_type.try_into()?);
        if c.nullable {
            cd = cd.nullable();
        }
        if let Some(ref_table) = c.references {
            cd = cd.references(&ref_table);
        }
        Ok(cd)
    }
}

impl TryFrom<JsTableSchema> for jazz_tools::query_manager::types::TableSchema {
    type Error = JazzRnError;

    fn try_from(js: JsTableSchema) -> Result<Self, Self::Error> {
        use jazz_tools::query_manager::types::{RowDescriptor, TableSchema};

        let columns = js
            .columns
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<jazz_tools::query_manager::types::ColumnDescriptor>, _>>()?;
        Ok(TableSchema::new(RowDescriptor::new(columns)))
    }
}

impl TryFrom<JsSchema> for Schema {
    type Error = JazzRnError;

    fn try_from(js: JsSchema) -> Result<Self, Self::Error> {
        use jazz_tools::query_manager::types::TableName;

        let mut schema = Schema::new();
        for (table_name, table_schema) in js.tables {
            schema.insert(TableName::new(&table_name), table_schema.try_into()?);
        }
        Ok(schema)
    }
}

fn parse_query(query_json: &str) -> Result<Query, JazzRnError> {
    serde_json::from_str(query_json).map_err(json_err)
}

fn parse_session(session_json: Option<String>) -> Result<Option<Session>, JazzRnError> {
    match session_json {
        Some(json) => Ok(Some(serde_json::from_str(&json).map_err(json_err)?)),
        None => Ok(None),
    }
}

fn parse_tier(tier: &str) -> Result<PersistenceTier, JazzRnError> {
    match tier {
        "worker" => Ok(PersistenceTier::Worker),
        "edge" => Ok(PersistenceTier::EdgeServer),
        "core" => Ok(PersistenceTier::CoreServer),
        _ => Err(JazzRnError::InvalidTier {
            message: format!(
                "Invalid tier '{}'. Must be 'worker', 'edge', or 'core'.",
                tier
            ),
        }),
    }
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
    fn on_sync_message(&self, message_json: String);
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
    fn send_sync_message(&self, message: OutboxEntry) {
        let Ok(json) = serde_json::to_string(&message) else {
            return;
        };

        if let Ok(guard) = self.callback.lock() {
            if let Some(cb) = guard.as_ref() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    cb.on_sync_message(json);
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
            let js_schema: JsSchema = serde_json::from_str(&schema_json).map_err(json_err)?;
            let schema: Schema = js_schema.try_into()?;

            let persistence_tier = tier.as_deref().map(parse_tier).transpose()?;

            let mut sync_manager = SyncManager::new();
            if let Some(t) = persistence_tier {
                sync_manager = sync_manager.with_tier(t);
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
            let id = core.insert(&table, values, None).map_err(runtime_err)?;
            Ok(id.uuid().to_string())
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
        settled_tier: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("query", || {
            let query = parse_query(&query_json)?;
            let session = parse_session(session_json)?;
            let tier = settled_tier.as_deref().map(parse_tier).transpose()?;

            // NOTE: query() triggers immediate_tick() internally.
            // We then block for the first callback result to be delivered.
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let fut = core.query(query, session, tier);
            let results = block_on(fut).map_err(runtime_err)?;

            let rows_json: Vec<serde_json::Value> = results
                .into_iter()
                .map(|(id, values)| {
                    let js_values: Vec<RnValue> = values.into_iter().map(Into::into).collect();
                    serde_json::json!({
                        "id": id.uuid().to_string(),
                        "values": js_values,
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
        settled_tier: Option<String>,
    ) -> Result<u64, JazzRnError> {
        with_panic_boundary("subscribe", || {
            let query = parse_query(&query_json)?;
            let session = parse_session(session_json)?;
            let tier = settled_tier.as_deref().map(parse_tier).transpose()?;

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;

            let handle = core
                .subscribe_with_settled_tier(
                    query,
                    {
                        move |delta: SubscriptionDelta| {
                            let descriptor = &delta.descriptor;
                            let row_to_json =
                                |row: &jazz_tools::query_manager::types::Row| -> serde_json::Value {
                                    let values = decode_row(descriptor, &row.data)
                                        .map(|vals| {
                                            vals.into_iter().map(RnValue::from).collect::<Vec<_>>()
                                        })
                                        .unwrap_or_default();
                                    serde_json::json!({
                                        "id": row.id.uuid().to_string(),
                                        "values": values,
                                    })
                                };

                            let payload = build_rn_delta_json(&delta, row_to_json);

                            if let Ok(json) = serde_json::to_string(&payload) {
                                let _ =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        callback.on_update(json);
                                    }));
                            }
                        }
                    },
                    session,
                    tier,
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

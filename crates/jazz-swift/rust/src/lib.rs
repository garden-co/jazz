//! jazz-swift (Rust) — UniFFI surface for Apple-platform Swift clients.
//!
//! The goal is a thin, JSON-friendly runtime boundary that mirrors the
//! already-proven `jazz-rn` surface closely enough for the app-side `JazzData`
//! layer to stay small and boring.

uniffi::setup_scaffolding!();

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::executor::block_on;

use jazz_tools::binding_support::{
    align_query_rows_to_declared_schema, align_row_values_to_declared_schema,
    current_timestamp_ms as binding_current_timestamp_ms,
    default_read_durability_options as default_binding_read_durability_options,
    generate_id as generate_binding_id, parse_durability_tier as parse_binding_tier,
    parse_query_input, parse_session_input, parse_write_context_input,
    query_rows_can_be_schema_aligned, serialize_outbox_entry, subscription_delta_to_json,
};
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::{Session, WriteContext};
use jazz_tools::query_manager::types::{Schema, SchemaHash, TableName, Value};
use jazz_tools::runtime_core::{
    ReadDurabilityOptions, RuntimeCore, Scheduler, SubscriptionDelta, SubscriptionHandle,
    SyncSender,
};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::{SqliteStorage, Storage};
use jazz_tools::sync_manager::{
    DurabilityTier, InboxEntry, OutboxEntry, QueryPropagation, ServerId, Source, SyncManager,
    SyncPayload,
};

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum JazzSwiftError {
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

fn json_err(e: serde_json::Error) -> JazzSwiftError {
    JazzSwiftError::InvalidJson {
        message: e.to_string(),
    }
}

fn runtime_err<E: std::fmt::Display>(e: E) -> JazzSwiftError {
    JazzSwiftError::Runtime {
        message: e.to_string(),
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

fn with_panic_boundary<T, F>(context: &'static str, f: F) -> Result<T, JazzSwiftError>
where
    F: FnOnce() -> Result<T, JazzSwiftError>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let panic_message = panic_payload_to_string(payload);
            let backtrace = std::backtrace::Backtrace::force_capture();
            Err(JazzSwiftError::Internal {
                message: format!("panic in {context}: {panic_message}\n{backtrace}"),
            })
        }
    }
}

fn convert_insert_values(values_json: &str) -> Result<HashMap<String, Value>, JazzSwiftError> {
    serde_json::from_str(values_json).map_err(json_err)
}

fn convert_updates(values_json: &str) -> Result<Vec<(String, Value)>, JazzSwiftError> {
    let partial: HashMap<String, Value> = serde_json::from_str(values_json).map_err(json_err)?;
    Ok(partial.into_iter().collect())
}

fn parse_query(query_json: &str) -> Result<Query, JazzSwiftError> {
    parse_query_input(query_json).map_err(|message| JazzSwiftError::InvalidJson { message })
}

fn parse_session(session_json: Option<String>) -> Result<Option<Session>, JazzSwiftError> {
    parse_session_input(session_json.as_deref())
        .map_err(|message| JazzSwiftError::InvalidJson { message })
}

fn parse_write_context(
    write_context_json: Option<String>,
) -> Result<Option<WriteContext>, JazzSwiftError> {
    parse_write_context_input(write_context_json.as_deref())
        .map_err(|message| JazzSwiftError::InvalidJson { message })
}

fn parse_tier(tier: &str) -> Result<DurabilityTier, JazzSwiftError> {
    parse_binding_tier(tier).map_err(|message| JazzSwiftError::InvalidTier { message })
}

fn default_read_durability_options(tier: Option<DurabilityTier>) -> ReadDurabilityOptions {
    default_binding_read_durability_options(tier)
}

fn parse_subscription_inputs(
    query_json: &str,
    session_json: Option<String>,
    tier: Option<String>,
) -> Result<(Query, Option<Session>, ReadDurabilityOptions), JazzSwiftError> {
    let query = parse_query(query_json)?;
    let session = parse_session(session_json)?;
    let tier = tier.as_deref().map(parse_tier).transpose()?;
    Ok((query, session, default_read_durability_options(tier)))
}

fn make_subscription_callback(
    callback: Box<dyn SubscriptionCallback>,
    declared_schema: Option<Schema>,
    table: Option<TableName>,
) -> impl Fn(SubscriptionDelta) + Send + 'static {
    move |delta: SubscriptionDelta| {
        let payload = subscription_delta_to_json(&delta, declared_schema.as_ref(), table.as_ref());
        if let Ok(json) = serde_json::to_string(&payload) {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                callback.on_update(json);
            }));
        }
    }
}

#[uniffi::export(callback_interface)]
pub trait BatchedTickCallback: Send + Sync {
    fn request_batched_tick(&self);
}

#[uniffi::export(callback_interface)]
pub trait SyncMessageCallback: Send + Sync {
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
    fn on_update(&self, delta_json: String);
}

#[derive(Clone, Default)]
struct SwiftScheduler {
    scheduled: Arc<AtomicBool>,
    callback: Arc<Mutex<Option<Box<dyn BatchedTickCallback>>>>,
}

impl SwiftScheduler {
    fn set_callback(&self, cb: Option<Box<dyn BatchedTickCallback>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = cb;
        }
    }

    fn clear_scheduled(&self) {
        self.scheduled.store(false, Ordering::SeqCst);
    }
}

impl Scheduler for SwiftScheduler {
    fn schedule_batched_tick(&self) {
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

            if !called {
                self.scheduled.store(false, Ordering::SeqCst);
            }
        }
    }
}

#[derive(Clone, Default)]
struct SwiftSyncSender {
    callback: Arc<Mutex<Option<Box<dyn SyncMessageCallback>>>>,
}

impl SwiftSyncSender {
    fn set_callback(&self, cb: Option<Box<dyn SyncMessageCallback>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = cb;
        }
    }
}

impl SyncSender for SwiftSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        let Ok(serialized) = serialize_outbox_entry(&message) else {
            return;
        };

        if let Ok(guard) = self.callback.lock() {
            if let Some(cb) = guard.as_ref() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    cb.on_sync_message(
                        serialized.destination_kind,
                        serialized.destination_id,
                        serialized.payload_json,
                        serialized.is_catalogue,
                    );
                }));
            }
        }
    }
}

type SwiftCoreType = RuntimeCore<SqliteStorage, SwiftScheduler, SwiftSyncSender>;

#[derive(uniffi::Object)]
pub struct JazzSwiftRuntime {
    core: Mutex<SwiftCoreType>,
    upstream_server_id: Mutex<Option<ServerId>>,
    declared_schema: Schema,
    subscription_queries: Mutex<HashMap<u64, Query>>,
}

#[uniffi::export]
impl JazzSwiftRuntime {
    #[uniffi::constructor]
    pub fn new(
        schema_json: String,
        app_id: String,
        jazz_env: String,
        user_branch: String,
        tier: Option<String>,
        data_path: Option<String>,
    ) -> Result<Arc<Self>, JazzSwiftError> {
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
                    .map_err(|e| JazzSwiftError::Schema {
                        message: format!("{:?}", e),
                    })?;

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
                default_path.push(format!("{sanitized_app_id}.sqlite"));
                default_path.to_string_lossy().into_owned()
            });
            let storage =
                SqliteStorage::open(&resolved_data_path).map_err(|e| JazzSwiftError::Runtime {
                    message: format!(
                        "Failed to open SQLite storage at '{}': {:?}",
                        resolved_data_path, e
                    ),
                })?;
            let scheduler = SwiftScheduler::default();
            let sync_sender = SwiftSyncSender::default();

            let mut core = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);
            core.persist_schema();

            Ok(Arc::new(Self {
                core: Mutex::new(core),
                upstream_server_id: Mutex::new(None),
                declared_schema,
                subscription_queries: Mutex::new(HashMap::new()),
            }))
        })
    }

    pub fn on_batched_tick_needed(
        &self,
        callback: Option<Box<dyn BatchedTickCallback>>,
    ) -> Result<(), JazzSwiftError> {
        with_panic_boundary("on_batched_tick_needed", || {
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.scheduler_mut().set_callback(callback);
            Ok(())
        })
    }

    pub fn on_sync_message_to_send(
        &self,
        callback: Option<Box<dyn SyncMessageCallback>>,
    ) -> Result<(), JazzSwiftError> {
        with_panic_boundary("on_sync_message_to_send", || {
            let core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.sync_sender().set_callback(callback);
            Ok(())
        })
    }

    pub fn batched_tick(&self) -> Result<(), JazzSwiftError> {
        with_panic_boundary("batched_tick", || {
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.scheduler_mut().clear_scheduled();
            core.batched_tick();
            Ok(())
        })
    }

    pub fn insert(&self, table: String, values_json: String) -> Result<String, JazzSwiftError> {
        with_panic_boundary("insert", || {
            let named_values = convert_insert_values(&values_json)?;
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            let (id, row_values) = core
                .insert(&table, named_values, None)
                .map_err(runtime_err)?;
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
            .map_err(|e| JazzSwiftError::Internal {
                message: format!("insert serialization failed: {e}"),
            })
        })
    }

    pub fn insert_with_session(
        &self,
        table: String,
        values_json: String,
        write_context_json: Option<String>,
    ) -> Result<String, JazzSwiftError> {
        with_panic_boundary("insert_with_session", || {
            let named_values = convert_insert_values(&values_json)?;
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            let (id, row_values) = core
                .insert(&table, named_values, write_context.as_ref())
                .map_err(runtime_err)?;
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
            .map_err(|e| JazzSwiftError::Internal {
                message: format!("insert serialization failed: {e}"),
            })
        })
    }

    pub fn update(&self, object_id: String, values_json: String) -> Result<(), JazzSwiftError> {
        with_panic_boundary("update", || {
            let uuid =
                uuid::Uuid::parse_str(&object_id).map_err(|e| JazzSwiftError::InvalidUuid {
                    message: e.to_string(),
                })?;
            let oid = ObjectId::from_uuid(uuid);
            let updates = convert_updates(&values_json)?;
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.update(oid, updates, None).map_err(runtime_err)?;
            Ok(())
        })
    }

    pub fn update_with_session(
        &self,
        object_id: String,
        values_json: String,
        write_context_json: Option<String>,
    ) -> Result<(), JazzSwiftError> {
        with_panic_boundary("update_with_session", || {
            let uuid =
                uuid::Uuid::parse_str(&object_id).map_err(|e| JazzSwiftError::InvalidUuid {
                    message: e.to_string(),
                })?;
            let oid = ObjectId::from_uuid(uuid);
            let updates = convert_updates(&values_json)?;
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.update(oid, updates, write_context.as_ref())
                .map_err(runtime_err)?;
            Ok(())
        })
    }

    #[uniffi::method(name = "delete")]
    pub fn delete_row(&self, object_id: String) -> Result<(), JazzSwiftError> {
        with_panic_boundary("delete", || {
            let uuid =
                uuid::Uuid::parse_str(&object_id).map_err(|e| JazzSwiftError::InvalidUuid {
                    message: e.to_string(),
                })?;
            let oid = ObjectId::from_uuid(uuid);
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.delete(oid, None).map_err(runtime_err)?;
            Ok(())
        })
    }

    #[uniffi::method(name = "deleteWithSession")]
    pub fn delete_with_session(
        &self,
        object_id: String,
        write_context_json: Option<String>,
    ) -> Result<(), JazzSwiftError> {
        with_panic_boundary("delete_with_session", || {
            let uuid =
                uuid::Uuid::parse_str(&object_id).map_err(|e| JazzSwiftError::InvalidUuid {
                    message: e.to_string(),
                })?;
            let oid = ObjectId::from_uuid(uuid);
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.delete(oid, write_context.as_ref())
                .map_err(runtime_err)?;
            Ok(())
        })
    }

    pub fn query(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<String, JazzSwiftError> {
        with_panic_boundary("query", || {
            let query = parse_query(&query_json)?;
            let query_for_alignment = query.clone();
            let session = parse_session(session_json)?;
            let tier = tier.as_deref().map(parse_tier).transpose()?;

            let (fut, runtime_schema) = {
                let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                    message: "lock poisoned".into(),
                })?;
                (
                    core.query_with_propagation(
                        query,
                        session,
                        default_read_durability_options(tier),
                        QueryPropagation::Full,
                    ),
                    core.current_schema().clone(),
                )
            };
            let results = block_on(fut).map_err(runtime_err)?;
            let results = align_query_rows_to_declared_schema(
                &self.declared_schema,
                &runtime_schema,
                &query_for_alignment,
                results,
            );

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

    pub fn subscribe(
        &self,
        query_json: String,
        callback: Box<dyn SubscriptionCallback>,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<u64, JazzSwiftError> {
        with_panic_boundary("subscribe", || {
            let (query, session, durability) =
                parse_subscription_inputs(&query_json, session_json, tier)?;
            let alignment_table = if query_rows_can_be_schema_aligned(&query) {
                Some(query.table)
            } else {
                None
            };
            let callback = make_subscription_callback(
                callback,
                alignment_table
                    .as_ref()
                    .map(|_| self.declared_schema.clone()),
                alignment_table,
            );

            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
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

    pub fn unsubscribe(&self, handle: u64) -> Result<(), JazzSwiftError> {
        with_panic_boundary("unsubscribe", || {
            self.subscription_queries
                .lock()
                .map_err(|_| JazzSwiftError::Internal {
                    message: "lock poisoned".into(),
                })?
                .remove(&handle);
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.unsubscribe(SubscriptionHandle(handle));
            Ok(())
        })
    }

    pub fn create_subscription(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<u64, JazzSwiftError> {
        with_panic_boundary("create_subscription", || {
            let (query, session, durability) =
                parse_subscription_inputs(&query_json, session_json, tier)?;
            let query_for_alignment = query.clone();

            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;

            let handle =
                core.create_subscription(query, session, durability, QueryPropagation::Full);
            drop(core);

            if query_rows_can_be_schema_aligned(&query_for_alignment) {
                self.subscription_queries
                    .lock()
                    .map_err(|_| JazzSwiftError::Internal {
                        message: "lock poisoned".into(),
                    })?
                    .insert(handle.0, query_for_alignment);
            }

            Ok(handle.0)
        })
    }

    pub fn execute_subscription(
        &self,
        handle: u64,
        callback: Box<dyn SubscriptionCallback>,
    ) -> Result<(), JazzSwiftError> {
        with_panic_boundary("execute_subscription", || {
            let alignment_table = self
                .subscription_queries
                .lock()
                .map_err(|_| JazzSwiftError::Internal {
                    message: "lock poisoned".into(),
                })?
                .get(&handle)
                .map(|query| query.table);
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            let callback = make_subscription_callback(
                callback,
                alignment_table
                    .as_ref()
                    .map(|_| self.declared_schema.clone()),
                alignment_table,
            );

            core.execute_subscription(SubscriptionHandle(handle), callback)
                .map_err(runtime_err)?;

            Ok(())
        })
    }

    pub fn on_sync_message_received(&self, message_json: String) -> Result<(), JazzSwiftError> {
        with_panic_boundary("on_sync_message_received", || {
            let payload: SyncPayload = serde_json::from_str(&message_json).map_err(json_err)?;
            let entry = InboxEntry {
                source: Source::Server(ServerId::new()),
                payload,
            };
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.park_sync_message(entry);
            Ok(())
        })
    }

    pub fn add_server(&self) -> Result<(), JazzSwiftError> {
        with_panic_boundary("add_server", || {
            let server_id = {
                let mut slot =
                    self.upstream_server_id
                        .lock()
                        .map_err(|_| JazzSwiftError::Internal {
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

            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.remove_server(server_id);
            core.add_server(server_id);
            Ok(())
        })
    }

    pub fn remove_server(&self) -> Result<(), JazzSwiftError> {
        with_panic_boundary("remove_server", || {
            let server_id = {
                let slot =
                    self.upstream_server_id
                        .lock()
                        .map_err(|_| JazzSwiftError::Internal {
                            message: "lock poisoned".into(),
                        })?;
                *slot
            };
            let Some(server_id) = server_id else {
                return Ok(());
            };
            let mut core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.remove_server(server_id);
            Ok(())
        })
    }

    pub fn get_schema_hash(&self) -> Result<String, JazzSwiftError> {
        with_panic_boundary("get_schema_hash", || {
            let core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            let schema = core.current_schema();
            Ok(SchemaHash::compute(schema).to_string())
        })
    }

    pub fn flush(&self) -> Result<(), JazzSwiftError> {
        with_panic_boundary("flush", || {
            let core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.flush_storage();
            Ok(())
        })
    }

    pub fn close(&self) -> Result<(), JazzSwiftError> {
        with_panic_boundary("close", || {
            let core = self.core.lock().map_err(|_| JazzSwiftError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.flush_storage();
            core.storage().close().map_err(runtime_err)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use jazz_tools::binding_support::{
        align_query_rows_to_declared_schema, align_values_to_declared_schema,
        query_rows_can_be_schema_aligned,
    };
    use jazz_tools::object::ObjectId;
    use jazz_tools::query_manager::query::Query;
    use jazz_tools::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaBuilder, TableName, TableSchema,
        Value,
    };

    fn declared_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("description", ColumnType::Text),
            )
            .build()
    }

    fn runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("description", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("title", ColumnType::Text),
            )
            .build()
    }

    #[test]
    fn query_rows_are_reordered_back_to_declared_schema() {
        let rows = vec![(
            ObjectId::new(),
            vec![
                Value::Text("note".to_string()),
                Value::Boolean(false),
                Value::Text("buy milk".to_string()),
            ],
        )];
        let query = Query::new("todos");

        let aligned = align_query_rows_to_declared_schema(
            &declared_todo_schema(),
            &runtime_todo_schema(),
            &query,
            rows,
        );

        assert_eq!(
            aligned[0].1,
            vec![
                Value::Text("buy milk".to_string()),
                Value::Boolean(false),
                Value::Text("note".to_string()),
            ]
        );
    }

    #[test]
    fn descriptor_values_are_reordered_back_to_declared_schema() {
        let runtime_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("description", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);

        let aligned = align_values_to_declared_schema(
            &declared_todo_schema(),
            &TableName::new("todos"),
            &runtime_descriptor,
            vec![
                Value::Text("note".to_string()),
                Value::Boolean(true),
                Value::Text("ship fix".to_string()),
            ],
        );

        assert_eq!(
            aligned,
            vec![
                Value::Text("ship fix".to_string()),
                Value::Boolean(true),
                Value::Text("note".to_string()),
            ]
        );
    }

    #[test]
    fn simple_queries_are_schema_alignable() {
        assert!(query_rows_can_be_schema_aligned(&Query::new("todos")));
    }
}

#[uniffi::export]
pub fn generate_id() -> String {
    generate_binding_id()
}

#[uniffi::export]
pub fn current_timestamp_ms() -> i64 {
    binding_current_timestamp_ms()
}

// jazz-rn (Rust) — UniFFI surface for React Native.
//
// Note: This crate intentionally uses UniFFI proc-macros (no UDL). The RN bindings
// generator runs UniFFI in "library mode", reading this crate's metadata.
uniffi::setup_scaffolding!();

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::future::FutureExt;
use serde::Deserialize;

use jazz_tools::binding_support::{
    default_read_durability_options as default_binding_read_durability_options,
    parse_batch_id_input, parse_batch_mode_input, parse_durability_tier as parse_binding_tier,
    parse_external_object_id, parse_query_input, parse_read_durability_options,
    parse_session_input, parse_write_context_input, serialize_mutation_error_event,
    subscription_delta_to_json,
};
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::{Session, WriteContext};
use jazz_tools::query_manager::types::{Schema, SchemaHash, Value};
use jazz_tools::runtime_core::{
    MutationErrorCallback as CoreMutationErrorCallback, ReadDurabilityOptions, RuntimeCore,
    Scheduler, SubscriptionDelta, SubscriptionHandle,
};
use jazz_tools::schema_manager::{rehydrate_schema_manager_from_catalogue, AppId, SchemaManager};
use jazz_tools::storage::{SqliteStorage, Storage};
use jazz_tools::sync_manager::{DurabilityTier, QueryPropagation, SyncManager};

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

fn runtime_err<E: std::fmt::Display>(e: E) -> JazzRnError {
    JazzRnError::Runtime {
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

fn panic_to_jazz_error(
    context: &'static str,
    payload: Box<dyn std::any::Any + Send>,
) -> JazzRnError {
    let panic_message = panic_payload_to_string(payload);
    let backtrace = std::backtrace::Backtrace::force_capture();
    JazzRnError::Internal {
        message: format!("panic in {context}: {panic_message}\n{backtrace}"),
    }
}

fn with_panic_boundary<T, F>(context: &'static str, f: F) -> Result<T, JazzRnError>
where
    F: FnOnce() -> Result<T, JazzRnError>,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .unwrap_or_else(|payload| Err(panic_to_jazz_error(context, payload)))
}

async fn with_async_panic_boundary<T, F, Fut>(context: &'static str, f: F) -> Result<T, JazzRnError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, JazzRnError>>,
{
    std::panic::AssertUnwindSafe(f())
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| Err(panic_to_jazz_error(context, payload)))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "value")]
enum FfiJsonValue {
    Integer(i32),
    BigInt(i64),
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    Bytea(String),
    Array(Vec<FfiJsonValue>),
    Row(FfiJsonRow),
    Null,
}

#[derive(Debug, Clone, Deserialize)]
struct FfiJsonRow {
    #[serde(default)]
    id: Option<ObjectId>,
    values: Vec<FfiJsonValue>,
}

fn ffi_json_err(message: impl Into<String>) -> JazzRnError {
    JazzRnError::InvalidJson {
        message: message.into(),
    }
}

fn decode_ffi_json_value(value: FfiJsonValue) -> Result<Value, JazzRnError> {
    match value {
        FfiJsonValue::Integer(value) => Ok(Value::Integer(value)),
        FfiJsonValue::BigInt(value) => Ok(Value::BigInt(value)),
        FfiJsonValue::Double(value) => Ok(Value::Double(value)),
        FfiJsonValue::Boolean(value) => Ok(Value::Boolean(value)),
        FfiJsonValue::Text(value) => Ok(Value::Text(value)),
        FfiJsonValue::Timestamp(value) => Ok(Value::Timestamp(value)),
        FfiJsonValue::Uuid(value) => Ok(Value::Uuid(value)),
        FfiJsonValue::Bytea(value) => hex::decode(value)
            .map(Value::Bytea)
            .map_err(|error| ffi_json_err(format!("invalid Bytea hex payload: {error}"))),
        FfiJsonValue::Array(values) => values
            .into_iter()
            .map(decode_ffi_json_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        FfiJsonValue::Row(row) => row
            .values
            .into_iter()
            .map(decode_ffi_json_value)
            .collect::<Result<Vec<_>, _>>()
            .map(|values| Value::Row { id: row.id, values }),
        FfiJsonValue::Null => Ok(Value::Null),
    }
}

fn decode_ffi_json_record(values_json: &str) -> Result<HashMap<String, Value>, JazzRnError> {
    let values: HashMap<String, FfiJsonValue> =
        serde_json::from_str(values_json).map_err(json_err)?;
    values
        .into_iter()
        .map(|(key, value)| decode_ffi_json_value(value).map(|value| (key, value)))
        .collect()
}

fn convert_insert_values(values_json: &str) -> Result<HashMap<String, Value>, JazzRnError> {
    decode_ffi_json_record(values_json)
}

fn convert_updates(values_json: &str) -> Result<Vec<(String, Value)>, JazzRnError> {
    let partial = decode_ffi_json_record(values_json)?;
    Ok(partial.into_iter().collect())
}

fn parse_query(query_json: &str) -> Result<Query, JazzRnError> {
    parse_query_input(query_json).map_err(|message| JazzRnError::InvalidJson { message })
}

fn parse_session(session_json: Option<String>) -> Result<Option<Session>, JazzRnError> {
    parse_session_input(session_json.as_deref())
        .map_err(|message| JazzRnError::InvalidJson { message })
}

fn parse_write_context(
    write_context_json: Option<String>,
) -> Result<Option<WriteContext>, JazzRnError> {
    parse_write_context_input(write_context_json.as_deref())
        .map_err(|message| JazzRnError::InvalidJson { message })
}

fn parse_tier(tier: &str) -> Result<DurabilityTier, JazzRnError> {
    parse_binding_tier(tier).map_err(|message| JazzRnError::InvalidTier { message })
}

fn default_read_durability_options(tier: Option<DurabilityTier>) -> ReadDurabilityOptions {
    default_binding_read_durability_options(tier)
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
pub trait SubscriptionCallback: Send + Sync {
    /// Called when a subscription produces an update.
    fn on_update(&self, delta_json: String);
}

#[uniffi::export(callback_interface)]
pub trait AuthFailureCallback: Send + Sync {
    /// Invoked when the Rust transport receives an auth rejection from the server.
    /// `reason` is a human-readable string (e.g. "Unauthorized").
    fn on_failure(&self, reason: String);
}

#[uniffi::export(callback_interface)]
pub trait MutationErrorCallback: Send + Sync {
    /// Invoked when a rejected local mutation was not handled by wait_for_batch.
    fn on_error(&self, event_json: String);
}

// ============================================================================
// RnScheduler
// ============================================================================

#[derive(Clone, Copy)]
enum SchedulerJob {
    Tick,
    DeliverMutationErrors,
}

struct SchedulerWorker {
    sender: Option<std::sync::mpsc::Sender<SchedulerJob>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl SchedulerWorker {
    fn send(&self, job: SchedulerJob) -> Result<(), std::sync::mpsc::SendError<SchedulerJob>> {
        let Some(sender) = self.sender.as_ref() else {
            return Err(std::sync::mpsc::SendError(job));
        };
        sender.send(job)
    }

    fn shutdown(&mut self) {
        drop(self.sender.take());

        let Some(handle) = self.handle.take() else {
            return;
        };

        if handle.thread().id() == std::thread::current().id() {
            return;
        }

        let _ = handle.join();
    }
}

impl Drop for SchedulerWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Clone, Default)]
struct RnScheduler {
    scheduled: Arc<AtomicBool>,
    mutation_error_delivery_scheduled: Arc<AtomicBool>,
    core_ref: Arc<Mutex<Option<Weak<Mutex<RnCoreType>>>>>,
    callback: Arc<Mutex<Option<Box<dyn BatchedTickCallback>>>>,
    worker: Arc<Mutex<Option<SchedulerWorker>>>,
    shutdown: Arc<AtomicBool>,
}

impl RnScheduler {
    fn set_core_ref(&self, core_ref: Weak<Mutex<RnCoreType>>) {
        if let Ok(mut slot) = self.core_ref.lock() {
            *slot = Some(core_ref);
        }
    }

    fn set_callback(&self, cb: Option<Box<dyn BatchedTickCallback>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = cb;
        }
    }

    fn clear_scheduled(&self) {
        self.scheduled.store(false, Ordering::SeqCst);
    }

    fn send_job(&self, job: SchedulerJob) {
        if self.shutdown.load(Ordering::SeqCst) {
            self.clear_job_scheduled(job);
            return;
        }

        let mut slot = match self.worker.lock() {
            Ok(slot) => slot,
            Err(_) => {
                self.clear_job_scheduled(job);
                return;
            }
        };

        if self.shutdown.load(Ordering::SeqCst) {
            self.clear_job_scheduled(job);
            return;
        }

        if slot.is_none() {
            *slot = Some(Self::spawn_worker(
                Arc::clone(&self.scheduled),
                Arc::clone(&self.mutation_error_delivery_scheduled),
                Arc::clone(&self.core_ref),
                Arc::clone(&self.callback),
            ));
        }

        let send_failed = slot
            .as_ref()
            .is_some_and(|worker| worker.send(job).is_err());

        if send_failed {
            self.clear_job_scheduled(job);
            let _ = slot.take();
        }
    }

    fn clear_job_scheduled(&self, job: SchedulerJob) {
        match job {
            SchedulerJob::Tick => self.scheduled.store(false, Ordering::SeqCst),
            SchedulerJob::DeliverMutationErrors => self
                .mutation_error_delivery_scheduled
                .store(false, Ordering::SeqCst),
        }
    }

    fn spawn_worker(
        scheduled: Arc<AtomicBool>,
        mutation_error_delivery_scheduled: Arc<AtomicBool>,
        core_ref: Arc<Mutex<Option<Weak<Mutex<RnCoreType>>>>>,
        callback: Arc<Mutex<Option<Box<dyn BatchedTickCallback>>>>,
    ) -> SchedulerWorker {
        let (sender, receiver) = std::sync::mpsc::channel::<SchedulerJob>();
        let handle = std::thread::Builder::new()
            .name("jazz-rn-scheduler".into())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    std::thread::sleep(Duration::from_millis(1));
                    match job {
                        SchedulerJob::Tick => {
                            scheduled.store(false, Ordering::SeqCst);
                            Self::request_batched_tick(&callback);
                        }
                        SchedulerJob::DeliverMutationErrors => {
                            mutation_error_delivery_scheduled.store(false, Ordering::SeqCst);
                            Self::deliver_mutation_errors(&core_ref);
                        }
                    }
                }
            })
            .expect("spawn jazz-rn scheduler thread");

        SchedulerWorker {
            sender: Some(sender),
            handle: Some(handle),
        }
    }

    fn request_batched_tick(callback: &Arc<Mutex<Option<Box<dyn BatchedTickCallback>>>>) {
        if let Ok(guard) = callback.lock() {
            if let Some(cb) = guard.as_ref() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    cb.request_batched_tick();
                }));
            }
        }
    }

    fn deliver_mutation_errors(core_ref: &Arc<Mutex<Option<Weak<Mutex<RnCoreType>>>>>) {
        let core_ref = core_ref.lock().ok().and_then(|slot| slot.clone());

        if let Some(core) = core_ref.and_then(|core_ref| core_ref.upgrade()) {
            if let Err(error) = deliver_pending_mutation_errors(&core) {
                eprintln!("jazz-rn: deliver pending mutation errors: {error:?}");
            }
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.scheduled.store(false, Ordering::SeqCst);
        self.mutation_error_delivery_scheduled
            .store(false, Ordering::SeqCst);

        if let Ok(mut slot) = self.worker.lock() {
            if let Some(mut worker) = slot.take() {
                worker.shutdown();
            }
        }
    }
}

impl Scheduler for RnScheduler {
    fn schedule_batched_tick(&self) {
        // Debounce: only one pending tick request at a time.
        if self.scheduled.swap(true, Ordering::SeqCst) {
            return;
        }

        // Defer firing the JS callback through the scheduler worker so we do
        // not synchronously re-enter `RnRuntime::batched_tick` from inside
        // `core.batched_tick()`. Without this delay,
        // `cb.request_batched_tick()` enqueues a JS microtask that runs
        // another `batched_tick` immediately, hot-looping the JS thread and
        // starving `setInterval`/render. The 1ms sleep also coalesces bursts
        // of schedule calls within a tick into a single follow-up callback.
        // This mirrors `schedule_mutation_error_delivery` below and
        // `NapiScheduler::schedule_batched_tick` in `jazz-napi`.
        self.send_job(SchedulerJob::Tick);
    }

    fn schedule_mutation_error_delivery(&self) {
        if self
            .mutation_error_delivery_scheduled
            .swap(true, Ordering::SeqCst)
        {
            return;
        }

        self.send_job(SchedulerJob::DeliverMutationErrors);
    }
}

// ============================================================================
// RnRuntime
// ============================================================================

type RnCoreType = RuntimeCore<SqliteStorage, RnScheduler>;

fn deliver_pending_mutation_errors(core: &Arc<Mutex<RnCoreType>>) -> Result<(), JazzRnError> {
    let delivery = {
        let mut core = core.lock().map_err(|_| JazzRnError::Internal {
            message: "lock poisoned".into(),
        })?;
        core.pending_mutation_error_delivery()
    };

    let Some((callback, events)) = delivery else {
        return Ok(());
    };

    for event in events {
        callback(&event);
    }

    Ok(())
}

#[derive(uniffi::Object)]
pub struct RnRuntime {
    core: Arc<Mutex<RnCoreType>>,
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

            let persistence_tier = tier.as_deref().map(parse_tier).transpose()?;

            let mut sync_manager = SyncManager::new();
            if let Some(t) = persistence_tier {
                sync_manager = sync_manager.with_durability_tier(t);
            }

            let app_id_obj =
                AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id));
            let mut schema_manager =
                SchemaManager::new(sync_manager, schema, app_id_obj, &jazz_env, &user_branch)
                    .map_err(|e| JazzRnError::Schema {
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
                SqliteStorage::open(&resolved_data_path).map_err(|e| JazzRnError::Runtime {
                    message: format!(
                        "Failed to open SQLite storage at '{}': {:?}",
                        resolved_data_path, e
                    ),
                })?;

            // Load previously-persisted schema history, permissions bundle, and lens
            // catalogue entries from storage into the in-memory schema manager so
            // offline cold-starts can decode and serve locally stored rows.
            if let Err(error) =
                rehydrate_schema_manager_from_catalogue(&mut schema_manager, &storage, app_id_obj)
            {
                eprintln!(
                    "jazz-rn: failed to rehydrate schema manager from catalogue storage for app {app_id_obj}: {error}"
                );
            }

            let scheduler = RnScheduler::default();

            let mut core = RuntimeCore::new(schema_manager, storage, scheduler);
            core.persist_schema();
            let core = Arc::new(Mutex::new(core));
            {
                let core_guard = core.lock().map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?;
                core_guard.scheduler().set_core_ref(Arc::downgrade(&core));
            }

            Ok(Arc::new(Self { core }))
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

    /// Run a batched tick. JS should call this when asked via `on_batched_tick_needed`.
    pub fn batched_tick(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("batched_tick", || {
            {
                let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?;
                core.scheduler_mut().clear_scheduled();
                core.batched_tick();
                if let Some(error) = core.take_storage_flush_error() {
                    return Err(runtime_err(format!("storage WAL flush failed: {error}")));
                }
            }
            Ok(())
        })
    }

    // =========================================================================
    // CRUD
    // =========================================================================

    pub fn insert(
        &self,
        table: String,
        values_json: String,
        write_context_json: Option<String>,
        object_id: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("insert", || {
            let named_values = convert_insert_values(&values_json)?;
            let write_context = parse_write_context(write_context_json)?;
            let object_id = parse_external_object_id(object_id.as_deref())
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let ((id, row_values), batch_id) = core
                .insert_with_id(&table, named_values, object_id, write_context.as_ref())
                .map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "id": id.uuid().to_string(),
                "values": row_values,
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("insert serialization failed: {e}"),
            })
        })
    }

    pub fn restore(
        &self,
        table: String,
        object_id: String,
        values_json: String,
        write_context_json: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("restore", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let named_values = convert_insert_values(&values_json)?;
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let ((id, row_values), batch_id) = core
                .restore(&table, oid, named_values, write_context.as_ref())
                .map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "id": id.uuid().to_string(),
                "values": row_values,
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("restore serialization failed: {e}"),
            })
        })
    }

    pub fn update(
        &self,
        object_id: String,
        values_json: String,
        write_context_json: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("update", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let updates = convert_updates(&values_json)?;
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let batch_id = core
                .update(oid, updates, write_context.as_ref())
                .map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("update serialization failed: {e}"),
            })
        })
    }

    pub fn upsert(
        &self,
        table: String,
        object_id: String,
        values_json: String,
        write_context_json: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("upsert", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let named_values = convert_insert_values(&values_json)?;
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let batch_id = core
                .upsert(&table, oid, named_values, write_context.as_ref())
                .map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("upsert serialization failed: {e}"),
            })
        })
    }

    pub fn begin_batch(&self, batch_mode: String) -> Result<String, JazzRnError> {
        with_panic_boundary("begin_batch", || {
            let batch_mode = parse_batch_mode_input(&batch_mode)
                .map_err(|message| JazzRnError::InvalidJson { message })?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            Ok(core.begin_batch(batch_mode).to_string())
        })
    }

    pub fn rollback_batch(&self, batch_id: String) -> Result<bool, JazzRnError> {
        with_panic_boundary("rollback_batch", || {
            let batch_id = parse_batch_id_input(&batch_id)
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.rollback_batch(batch_id).map_err(runtime_err)
        })
    }

    #[uniffi::method(name = "delete")]
    pub fn delete_row(
        &self,
        object_id: String,
        write_context_json: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("delete", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let batch_id = core
                .delete(oid, write_context.as_ref())
                .map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("delete serialization failed: {e}"),
            })
        })
    }

    /// Wait for a local batch to settle at the requested durability tier.
    pub async fn wait_for_batch(&self, batch_id: String, tier: String) -> Result<(), JazzRnError> {
        with_async_panic_boundary("wait_for_batch", || async move {
            let batch_id = parse_batch_id_input(&batch_id)
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let tier = parse_tier(&tier)?;
            let receiver = {
                let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?;
                core.wait_for_batch(batch_id, tier).map_err(runtime_err)?
            };

            match receiver.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(rejection)) => Err(JazzRnError::Runtime {
                    message: format!(
                        "Persisted batch {} was rejected ({}): {}",
                        rejection.batch_id, rejection.code, rejection.reason
                    ),
                }),
                Err(_) => Err(JazzRnError::Runtime {
                    message: "Wait for batch cancelled".into(),
                }),
            }
        })
        .await
    }

    // =========================================================================
    // Queries
    // =========================================================================

    /// One-shot query returning a JSON string:
    /// `[{ "id": "<uuid>", "values": [ {type, value}, ... ] }, ...]`.
    ///
    /// `async` so the JS thread is not blocked while the query future is
    /// waiting on a later `batched_tick` to settle (which is itself driven
    /// from JS via the `on_batched_tick_needed` callback). A synchronous
    /// `block_on` here can deadlock for any query that needs more than the
    /// inline `immediate_tick` to resolve.
    pub async fn query(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
        options_json: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_async_panic_boundary("query", || async move {
            let query = parse_query(&query_json)?;
            let session = parse_session(session_json)?;
            let (durability, propagation, transaction_batch_id) =
                parse_read_durability_options(tier.as_deref(), options_json.as_deref())
                    .map_err(|message| JazzRnError::InvalidJson { message })?;

            let fut = {
                let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?;
                core.query_with_local_batch(
                    query,
                    session,
                    durability,
                    propagation,
                    transaction_batch_id,
                )
                .map_err(runtime_err)?
            };
            let results = fut.await.map_err(runtime_err)?;

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
        .await
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

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

    pub fn on_mutation_error(
        &self,
        callback: Box<dyn MutationErrorCallback>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("on_mutation_error", || {
            let callback: CoreMutationErrorCallback = Arc::new(move |event| {
                let Ok(event_json) = serde_json::to_string(&serialize_mutation_error_event(event))
                else {
                    return;
                };
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    callback.on_error(event_json);
                }));
            });
            self.core
                .lock()
                .map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?
                .set_mutation_error_callback(Some(callback));
            Ok(())
        })
    }

    pub fn commit_batch(&self, batch_id: String) -> Result<(), JazzRnError> {
        with_panic_boundary("commit_batch", || {
            let batch_id = parse_batch_id_input(&batch_id)
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.commit_batch(batch_id).map_err(runtime_err)
        })
    }

    /// Flush and close the underlying storage, releasing filesystem locks.
    pub fn close(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("close", || {
            let (scheduler, flush_result, flush_wal_result, close_result) = {
                let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?;
                core.scheduler_mut().set_callback(None);
                core.set_mutation_error_callback(None);
                let scheduler = core.scheduler().clone();
                let flush_result = core.flush_storage();
                let flush_wal_result = core.flush_wal();
                let close_result = core.storage().close();

                (scheduler, flush_result, flush_wal_result, close_result)
            };

            scheduler.shutdown();

            flush_result.map_err(runtime_err)?;
            flush_wal_result.map_err(runtime_err)?;
            close_result.map_err(runtime_err)
        })
    }

    /// Connect to a Jazz server over WebSocket.
    ///
    /// Parses `auth_json` into `AuthConfig`, wires a `TransportManager` into
    /// `RuntimeCore`, and spawns the manager loop on a dedicated Tokio thread.
    pub fn connect(&self, url: String, auth_json: String) -> Result<(), JazzRnError> {
        with_panic_boundary("connect", || {
            let auth: jazz_tools::transport_manager::AuthConfig =
                serde_json::from_str(&auth_json).map_err(json_err)?;
            let scheduler = self
                .core
                .lock()
                .map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?
                .scheduler()
                .clone();
            let tick = RnTickNotifier { scheduler };
            let manager = {
                let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?;
                jazz_tools::runtime_core::install_transport::<
                    _,
                    _,
                    jazz_tools::ws_stream::NativeWsStream,
                    _,
                >(&mut core, url, auth, tick)
            };
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio rt");
                rt.block_on(manager.run());
            });
            Ok(())
        })
    }

    /// Disconnect from the Jazz server and drop the transport handle.
    pub fn disconnect(&self) {
        if let Ok(mut core) = self.core.lock() {
            let server_id = if let Some(handle) = core.transport() {
                handle.disconnect();
                Some(handle.server_id)
            } else {
                None
            };
            if let Some(server_id) = server_id {
                core.remove_server(server_id);
            }
            core.clear_transport();
        }
    }

    /// Push updated auth credentials into the live transport.
    pub fn update_auth(&self, auth_json: String) -> Result<(), JazzRnError> {
        with_panic_boundary("update_auth", || {
            let auth: jazz_tools::transport_manager::AuthConfig =
                serde_json::from_str(&auth_json).map_err(json_err)?;
            if let Ok(core) = self.core.lock() {
                if let Some(handle) = core.transport() {
                    handle.update_auth(auth);
                }
            }
            Ok(())
        })
    }

    /// Register a callback that fires when the transport receives an auth
    /// rejection from the server during the WS handshake.
    pub fn on_auth_failure(
        &self,
        callback: Box<dyn AuthFailureCallback>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("on_auth_failure", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.set_auth_failure_callback(move |reason| {
                callback.on_failure(reason);
            });
            Ok(())
        })
    }
}

// ============================================================================
// RnTickNotifier
// ============================================================================

/// `TickNotifier` implementation for the React Native (UniFFI) runtime.
///
/// Holds a clone of `RnScheduler` and calls `schedule_batched_tick()` whenever
/// the transport layer needs to wake up `batched_tick`.
struct RnTickNotifier {
    scheduler: RnScheduler,
}

impl jazz_tools::transport_manager::TickNotifier for RnTickNotifier {
    fn notify(&self) {
        self.scheduler.schedule_batched_tick();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread::ThreadId;

    struct BlockingTickCallback {
        invocations: AtomicUsize,
        entered_tx: mpsc::Sender<(usize, ThreadId)>,
        unblock_first_rx: Mutex<mpsc::Receiver<()>>,
    }

    impl BatchedTickCallback for BlockingTickCallback {
        fn request_batched_tick(&self) {
            let invocation = self.invocations.fetch_add(1, Ordering::SeqCst) + 1;
            self.entered_tx
                .send((invocation, std::thread::current().id()))
                .expect("test should receive tick callback");

            if invocation == 1 {
                self.unblock_first_rx
                    .lock()
                    .expect("test unblock receiver should not be poisoned")
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should unblock first tick callback");
            }
        }
    }

    #[test]
    fn scheduler_coalesces_bursts_on_one_worker_for_follow_up_ticks() {
        // This is intentionally an internal scheduler test: constant worker
        // count and callback serialization are not observable through the
        // public RN runtime API.
        let scheduler = RnScheduler::default();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (unblock_first_tx, unblock_first_rx) = mpsc::channel();

        scheduler.set_callback(Some(Box::new(BlockingTickCallback {
            invocations: AtomicUsize::new(0),
            entered_tx,
            unblock_first_rx: Mutex::new(unblock_first_rx),
        })));

        scheduler.schedule_batched_tick();
        let (first_invocation, first_thread_id) = entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first tick callback should run");
        assert_eq!(first_invocation, 1);

        for _ in 0..5 {
            scheduler.schedule_batched_tick();
        }
        assert!(
            entered_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "follow-up tick callback ran concurrently with the blocked callback"
        );

        unblock_first_tx
            .send(())
            .expect("first tick callback should still be waiting");
        let (second_invocation, second_thread_id) = entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("follow-up tick callback should run after the first returns");
        assert_eq!(second_invocation, 2);
        assert_eq!(
            second_thread_id, first_thread_id,
            "follow-up tick ran on a freshly spawned scheduler thread"
        );
        assert!(
            entered_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "burst schedules produced more than one follow-up tick"
        );
    }
}

// ============================================================================
// Module-level utilities
// ============================================================================

/// Mint a local-first JWT from a base64url-encoded 32-byte seed.
///
/// Returns a signed JWT that can be used as a bearer token for local-first auth.
/// `audience` should be the app ID (UUID) or a human-readable app name.
/// `ttl_seconds` controls token lifetime (e.g. 3600 for one hour).
#[uniffi::export]
pub fn mint_local_first_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: i64,
) -> Result<String, JazzRnError> {
    mint_token(
        seed_b64,
        audience,
        ttl_seconds,
        jazz_tools::identity::LOCAL_FIRST_ISSUER,
    )
}

/// Mint an anonymous JWT from a base64url-encoded 32-byte seed.
///
/// Returns a signed JWT that can be used as a bearer token for anonymous auth.
/// `audience` should be the app ID (UUID) or a human-readable app name.
/// `ttl_seconds` controls token lifetime (e.g. 3600 for one hour).
#[uniffi::export]
pub fn mint_anonymous_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: i64,
) -> Result<String, JazzRnError> {
    mint_token(
        seed_b64,
        audience,
        ttl_seconds,
        jazz_tools::identity::ANONYMOUS_ISSUER,
    )
}

fn mint_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: i64,
    issuer: &'static str,
) -> Result<String, JazzRnError> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&seed_b64)
        .map_err(|e| JazzRnError::Internal {
            message: format!("invalid base64 seed: {e}"),
        })?;
    let seed: [u8; 32] = bytes.try_into().map_err(|_| JazzRnError::Internal {
        message: "seed must be exactly 32 bytes".to_string(),
    })?;
    jazz_tools::identity::mint_jazz_self_signed_token(&seed, issuer, &audience, ttl_seconds as u64)
        .map_err(|e| JazzRnError::Internal { message: e })
}

//! jazz-napi — Native Node.js bindings for Jazz.
//!
//! Provides `NapiRuntime` wrapping `RuntimeCore<FjallStorage>` via napi-rs.
//! Exposed as the `jazz-napi` npm package for server-side TypeScript apps.
//!
//! # Architecture
//!
//! - `FjallStorage` provides persistent on-disk storage
//! - `NapiScheduler` implements `Scheduler` using `ThreadsafeFunction` to schedule
//!   `batched_tick()` on the Node.js event loop (debounced)
//! - `NapiSyncSender` implements `SyncSender` bridging to a JS callback
//! - `NapiRuntime` wraps `Arc<Mutex<RuntimeCore<...>>>`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;

use jazz_tools::binding_support::{
    align_query_rows_to_declared_schema, align_row_values_to_declared_schema, current_timestamp_ms,
    generate_id as generate_binding_id, parse_durability_tier as parse_binding_tier,
    parse_query_input, parse_read_durability_options as parse_binding_read_durability_options,
    parse_session_input, query_rows_can_be_schema_aligned, serialize_outbox_entry,
    subscription_delta_to_json,
};
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{Schema, SchemaHash, TableName, Value};
use jazz_tools::runtime_core::{
    ReadDurabilityOptions, RuntimeCore, Scheduler, SubscriptionDelta, SubscriptionHandle,
    SyncSender,
};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::{FjallStorage, MemoryStorage, Storage};
use jazz_tools::sync_manager::QueryPropagation;
use jazz_tools::sync_manager::{
    ClientId, DurabilityTier, InboxEntry, OutboxEntry, ServerId, Source, SyncManager, SyncPayload,
};

fn convert_values(values: Vec<Value>) -> Vec<Value> {
    values
}

fn convert_updates(partial: HashMap<String, Value>) -> Vec<(String, Value)> {
    partial.into_iter().collect()
}

fn parse_read_durability_options(
    tier: Option<String>,
    options_json: Option<String>,
) -> napi::Result<(ReadDurabilityOptions, QueryPropagation)> {
    parse_binding_read_durability_options(tier.as_deref(), options_json.as_deref())
        .map_err(napi::Error::from_reason)
}

fn parse_node_durability_tiers(tier: Option<&str>) -> napi::Result<Vec<DurabilityTier>> {
    let Some(raw) = tier else {
        return Ok(Vec::new());
    };
    Ok(vec![parse_tier(raw)?])
}

fn parse_node_durability_tier(tier: Option<String>) -> napi::Result<Vec<DurabilityTier>> {
    parse_node_durability_tiers(tier.as_deref())
}

fn open_fjall_storage_with_retry(data_path: &str, cache_size: usize) -> napi::Result<FjallStorage> {
    const MAX_ATTEMPTS: usize = 100;
    const RETRY_DELAY_MS: u64 = 25;

    let mut last_error = None;

    for attempt in 0..MAX_ATTEMPTS {
        match FjallStorage::open(data_path, cache_size) {
            Ok(storage) => return Ok(storage),
            Err(error) => {
                let is_lock_error = matches!(
                    &error,
                    jazz_tools::storage::StorageError::IoError(message)
                        if message.to_ascii_lowercase().contains("lock")
                            || message.to_ascii_lowercase().contains("busy")
                );
                if !is_lock_error || attempt + 1 == MAX_ATTEMPTS {
                    last_error = Some(error);
                    break;
                }
                thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
            }
        }
    }

    let error = last_error.unwrap_or_else(|| {
        jazz_tools::storage::StorageError::IoError(
            "fjall open failed without error details".to_string(),
        )
    });
    Err(napi::Error::from_reason(format!(
        "Failed to open storage: {:?}",
        error
    )))
}

// ============================================================================
fn parse_tier(tier: &str) -> napi::Result<DurabilityTier> {
    parse_binding_tier(tier).map_err(napi::Error::from_reason)
}

fn parse_query(json: &str) -> napi::Result<Query> {
    parse_query_input(json).map_err(napi::Error::from_reason)
}

fn parse_session_json(session_json: Option<String>) -> napi::Result<Option<Session>> {
    parse_session_input(session_json.as_deref())
        .map_err(|err| napi::Error::from_reason(format!("Invalid session JSON: {}", err)))
}

fn parse_subscription_inputs(
    query_json: &str,
    session_json: Option<String>,
    tier: Option<String>,
    options_json: Option<String>,
) -> napi::Result<(
    Query,
    Option<Session>,
    ReadDurabilityOptions,
    QueryPropagation,
)> {
    let query = parse_query(query_json)?;
    let session = parse_session_json(session_json)?;
    let (durability, propagation) = parse_read_durability_options(tier, options_json)?;
    Ok((query, session, durability, propagation))
}

fn make_subscription_callback(
    tsfn: ThreadsafeFunction<serde_json::Value>,
    declared_schema: Option<Schema>,
    table: Option<TableName>,
) -> impl Fn(SubscriptionDelta) + Send + 'static {
    move |delta: SubscriptionDelta| {
        tsfn.call(
            Ok(subscription_delta_to_json(
                &delta,
                declared_schema.as_ref(),
                table.as_ref(),
            )),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}

// ============================================================================
// NapiScheduler
// ============================================================================

type NapiCoreType = RuntimeCore<Box<dyn Storage + Send>, NapiScheduler, NapiSyncSender>;

/// Scheduler that schedules `batched_tick()` on the Node.js event loop via a
/// ThreadsafeFunction wrapping a noop JS function. The TSFN callback closure
/// does the actual work. Debounced: only one tick is pending at a time.
/// The TSFN type produced by `build_threadsafe_function().weak().build()`:
/// CalleeHandled = false, Weak = true (won't keep event loop alive).
type SchedulerTsfn = ThreadsafeFunction<(), (), (), napi::Status, false, true, 0>;

pub struct NapiScheduler {
    scheduled: Arc<AtomicBool>,
    core_ref: Weak<Mutex<NapiCoreType>>,
    tsfn: Option<SchedulerTsfn>,
}

impl NapiScheduler {
    fn new() -> Self {
        Self {
            scheduled: Arc::new(AtomicBool::new(false)),
            core_ref: Weak::new(),
            tsfn: None,
        }
    }

    fn set_core_ref(&mut self, core_ref: Weak<Mutex<NapiCoreType>>) {
        self.core_ref = core_ref;
    }

    fn set_tsfn(&mut self, tsfn: SchedulerTsfn) {
        self.tsfn = Some(tsfn);
    }
}

impl Scheduler for NapiScheduler {
    fn schedule_batched_tick(&self) {
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            if let Some(ref tsfn) = self.tsfn {
                // CalleeHandled = false: pass value directly, not wrapped in Result
                tsfn.call((), ThreadsafeFunctionCallMode::NonBlocking);
            } else {
                self.scheduled.store(false, Ordering::SeqCst);
            }
        }
    }
}

// ============================================================================
// NapiSyncSender
// ============================================================================

/// Arguments for the sync message callback
/// (destinationKind, destinationId, payloadJson, isCatalogue)
type SyncCallbackParams = (String, String, String, bool);

pub struct NapiSyncSender {
    callback: Arc<Mutex<Option<ThreadsafeFunction<SyncCallbackParams>>>>,
}

impl NapiSyncSender {
    fn new() -> Self {
        Self {
            callback: Arc::new(Mutex::new(None)),
        }
    }

    fn set_callback(&self, tsfn: ThreadsafeFunction<SyncCallbackParams>) {
        if let Ok(mut cb) = self.callback.lock() {
            *cb = Some(tsfn);
        }
    }
}

impl SyncSender for NapiSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        let cb = match self.callback.lock() {
            Ok(cb) => cb,
            Err(_) => return,
        };
        let tsfn = match cb.as_ref() {
            Some(tsfn) => tsfn,
            None => return,
        };
        let serialized = match serialize_outbox_entry(&message) {
            Ok(serialized) => serialized,
            Err(_) => return,
        };

        tsfn.call(
            Ok((
                serialized.destination_kind,
                serialized.destination_id,
                serialized.payload_json,
                serialized.is_catalogue,
            )),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}

fn build_napi_runtime(
    env: Env,
    schema_json: String,
    app_id: String,
    jazz_env: String,
    user_branch: String,
    storage: Box<dyn Storage + Send>,
    tier: Option<String>,
) -> napi::Result<NapiRuntime> {
    // Parse schema
    let schema: Schema = serde_json::from_str(&schema_json)
        .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
    let declared_schema = schema.clone();

    // Parse optional tier
    let node_tiers = parse_node_durability_tier(tier)?;

    // Create sync manager
    let mut sync_manager = SyncManager::new();
    if !node_tiers.is_empty() {
        sync_manager = sync_manager.with_durability_tiers(node_tiers);
    }

    // Create schema manager
    let schema_manager = SchemaManager::new(
        sync_manager,
        schema,
        AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id)),
        &jazz_env,
        &user_branch,
    )
    .map_err(|e| napi::Error::from_reason(format!("Failed to create SchemaManager: {:?}", e)))?;

    // Create components
    let scheduler = NapiScheduler::new();
    let sync_sender = NapiSyncSender::new();

    // Create RuntimeCore and wrap
    let core = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);
    let core_arc = Arc::new(Mutex::new(core));

    // Set up the scheduler's TSFN
    {
        let core_weak = Arc::downgrade(&core_arc);
        let scheduled_flag = {
            let core_guard = core_arc
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core_guard.scheduler().scheduled.clone()
        };

        let core_ref_for_tsfn = core_weak.clone();
        let flag_for_tsfn = scheduled_flag;

        let tick_fn = env.create_function_from_closure("__groove_tick", move |_ctx| {
            // Reset flag first so new ticks can be scheduled
            flag_for_tsfn.store(false, Ordering::SeqCst);
            if let Some(core_arc) = core_ref_for_tsfn.upgrade()
                && let Ok(mut core) = core_arc.lock()
            {
                core.batched_tick();
            }
            Ok(())
        })?;

        let tsfn = tick_fn.build_threadsafe_function().weak::<true>().build()?;

        // Set on scheduler
        let mut core_guard = core_arc
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core_guard.scheduler_mut().set_core_ref(core_weak);
        core_guard.scheduler_mut().set_tsfn(tsfn);

        // Persist schema to catalogue for server sync
        core_guard.persist_schema();
    }

    Ok(NapiRuntime {
        core: core_arc,
        upstream_server_id: Mutex::new(None),
        declared_schema,
        subscription_queries: Mutex::new(HashMap::new()),
    })
}

// ============================================================================
// NapiRuntime
// ============================================================================

#[napi]
pub struct NapiRuntime {
    core: Arc<Mutex<NapiCoreType>>,
    upstream_server_id: Mutex<Option<ServerId>>,
    declared_schema: Schema,
    subscription_queries: Mutex<HashMap<u64, Query>>,
}

#[napi]
impl NapiRuntime {
    /// Create a new NapiRuntime with Fjall-backed persistent storage.
    #[napi(constructor)]
    pub fn new(
        env: Env,
        schema_json: String,
        app_id: String,
        jazz_env: String,
        user_branch: String,
        data_path: String,
        tier: Option<String>,
    ) -> napi::Result<Self> {
        // Create FjallStorage
        let cache_size = 64 * 1024 * 1024; // 64MB default
        let storage = open_fjall_storage_with_retry(&data_path, cache_size)?;

        build_napi_runtime(
            env,
            schema_json,
            app_id,
            jazz_env,
            user_branch,
            Box::new(storage),
            tier,
        )
    }

    /// Create a new NapiRuntime with in-memory storage (no local persistence).
    #[napi(js_name = "inMemory")]
    pub fn in_memory(
        env: Env,
        schema_json: String,
        app_id: String,
        jazz_env: String,
        user_branch: String,
        tier: Option<String>,
    ) -> napi::Result<Self> {
        build_napi_runtime(
            env,
            schema_json,
            app_id,
            jazz_env,
            user_branch,
            Box::new(MemoryStorage::new()),
            tier,
        )
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    #[napi]
    pub fn insert(
        &self,
        table: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let js_values: Vec<Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let groove_values = convert_values(js_values);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let (object_id, row_values) = core
            .insert(&table, groove_values, None)
            .map_err(|e| napi::Error::from_reason(format!("Insert failed: {e}")))?;
        let row_values = align_row_values_to_declared_schema(
            &self.declared_schema,
            core.current_schema(),
            &TableName::new(table.clone()),
            row_values,
        );

        Ok(serde_json::json!({
            "id": object_id.uuid().to_string(),
            "values": row_values,
        }))
    }

    #[napi(js_name = "insertWithSession")]
    pub fn insert_with_session(
        &self,
        table: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        session_json: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let js_values: Vec<Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let groove_values = convert_values(js_values);
        let session = parse_session_json(session_json)?;

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let (object_id, row_values) = core
            .insert(&table, groove_values, session.as_ref())
            .map_err(|e| napi::Error::from_reason(format!("Insert failed: {:?}", e)))?;
        let row_values = align_row_values_to_declared_schema(
            &self.declared_schema,
            core.current_schema(),
            &TableName::new(table.clone()),
            row_values,
        );

        Ok(serde_json::json!({
            "id": object_id.uuid().to_string(),
            "values": row_values,
        }))
    }

    #[napi]
    pub fn update(
        &self,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
    ) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let updates = convert_updates(partial_values);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.update(oid, updates, None)
            .map_err(|e| napi::Error::from_reason(format!("Update failed: {e}")))?;

        Ok(())
    }

    #[napi(js_name = "updateWithSession")]
    pub fn update_with_session(
        &self,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        session_json: Option<String>,
    ) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let session = parse_session_json(session_json)?;

        let partial_values: HashMap<String, Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let updates = convert_updates(partial_values);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.update(oid, updates, session.as_ref())
            .map_err(|e| napi::Error::from_reason(format!("Update failed: {:?}", e)))?;

        Ok(())
    }

    #[napi(js_name = "delete")]
    pub fn delete_row(&self, object_id: String) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.delete(oid, None)
            .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?;

        Ok(())
    }

    #[napi(js_name = "deleteWithSession")]
    pub fn delete_with_session(
        &self,
        object_id: String,
        session_json: Option<String>,
    ) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let session = parse_session_json(session_json)?;

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.delete(oid, session.as_ref())
            .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Queries
    // =========================================================================

    #[napi(ts_return_type = "Promise<any>")]
    pub async fn query(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
        options_json: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let query = parse_query(&query_json)?;
        let query_for_alignment = query.clone();
        let session = parse_session_json(session_json)?;

        let (durability, propagation) = parse_read_durability_options(tier, options_json)?;

        let (future, runtime_schema) = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            (
                core.query_with_propagation(query, session, durability, propagation),
                core.current_schema().clone(),
            )
        };

        let rows = future
            .await
            .map_err(|e| napi::Error::from_reason(format!("Query failed: {:?}", e)))?;
        let rows = align_query_rows_to_declared_schema(
            &self.declared_schema,
            &runtime_schema,
            &query_for_alignment,
            rows,
        );

        let json_rows: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|(id, values)| {
                serde_json::json!({
                    "id": id.uuid().to_string(),
                    "values": values
                })
            })
            .collect();

        Ok(serde_json::Value::Array(json_rows))
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    #[napi]
    pub fn subscribe(
        &self,
        query_json: String,
        #[napi(ts_arg_type = "(...args: any[]) => any")] on_update: ThreadsafeFunction<
            serde_json::Value,
        >,
        session_json: Option<String>,
        tier: Option<String>,
        options_json: Option<String>,
    ) -> napi::Result<f64> {
        let (query, session, durability, propagation) =
            parse_subscription_inputs(&query_json, session_json, tier, options_json)?;
        let alignment_table = query_rows_can_be_schema_aligned(&query).then_some(query.table);

        let callback = make_subscription_callback(
            on_update,
            alignment_table
                .as_ref()
                .map(|_| self.declared_schema.clone()),
            alignment_table,
        );

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let handle = core
            .subscribe_with_durability_and_propagation(
                query,
                callback,
                session,
                durability,
                propagation,
            )
            .map_err(|e| napi::Error::from_reason(format!("Subscribe failed: {:?}", e)))?;

        Ok(handle.0 as f64)
    }

    #[napi]
    pub fn unsubscribe(&self, handle: f64) -> napi::Result<()> {
        self.subscription_queries
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .remove(&(handle as u64));
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.unsubscribe(SubscriptionHandle(handle as u64));
        Ok(())
    }

    /// Phase 1 of 2-phase subscribe: allocate a handle and store query params.
    #[napi(js_name = "createSubscription")]
    pub fn create_subscription(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
        options_json: Option<String>,
    ) -> napi::Result<f64> {
        let (query, session, durability, propagation) =
            parse_subscription_inputs(&query_json, session_json, tier, options_json)?;
        let query_for_alignment = query.clone();

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let handle = core.create_subscription(query, session, durability, propagation);
        drop(core);

        if query_rows_can_be_schema_aligned(&query_for_alignment) {
            self.subscription_queries
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?
                .insert(handle.0, query_for_alignment);
        }

        Ok(handle.0 as f64)
    }

    /// Phase 2 of 2-phase subscribe: compile, register, sync, attach callback, tick.
    #[napi(js_name = "executeSubscription")]
    pub fn execute_subscription(
        &self,
        handle: f64,
        #[napi(ts_arg_type = "(...args: any[]) => any")] on_update: ThreadsafeFunction<
            serde_json::Value,
        >,
    ) -> napi::Result<()> {
        let sub_handle = SubscriptionHandle(handle as u64);
        let alignment_table = self
            .subscription_queries
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .get(&(handle as u64))
            .map(|query| query.table);

        let callback = make_subscription_callback(
            on_update,
            alignment_table
                .as_ref()
                .map(|_| self.declared_schema.clone()),
            alignment_table,
        );

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.execute_subscription(sub_handle, callback)
            .map_err(|e| {
                napi::Error::from_reason(format!("Execute subscription failed: {:?}", e))
            })?;

        Ok(())
    }

    // =========================================================================
    // Persisted CRUD Operations
    // =========================================================================

    #[napi(js_name = "insertDurable", ts_return_type = "Promise<any>")]
    pub async fn insert_durable(
        &self,
        table: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        tier: String,
    ) -> napi::Result<serde_json::Value> {
        let persistence_tier = parse_tier(&tier)?;

        let js_values: Vec<Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let groove_values = convert_values(js_values);

        let ((object_id, row_values), receiver) = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            let ((object_id, row_values), receiver) = core
                .insert_persisted(&table, groove_values, None, persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Insert failed: {e}")))?;
            let row_values = align_row_values_to_declared_schema(
                &self.declared_schema,
                core.current_schema(),
                &TableName::new(table.clone()),
                row_values,
            );
            ((object_id, row_values), receiver)
        };

        let _ = receiver.await;
        Ok(serde_json::json!({
            "id": object_id.uuid().to_string(),
            "values": row_values,
        }))
    }

    #[napi(js_name = "insertDurableWithSession", ts_return_type = "Promise<any>")]
    pub async fn insert_durable_with_session(
        &self,
        table: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        session_json: Option<String>,
        tier: String,
    ) -> napi::Result<serde_json::Value> {
        let persistence_tier = parse_tier(&tier)?;
        let js_values: Vec<Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let groove_values = convert_values(js_values);
        let session = parse_session_json(session_json)?;

        let ((object_id, row_values), receiver) = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            let ((object_id, row_values), receiver) = core
                .insert_persisted(&table, groove_values, session.as_ref(), persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Insert failed: {:?}", e)))?;
            let row_values = align_row_values_to_declared_schema(
                &self.declared_schema,
                core.current_schema(),
                &TableName::new(table.clone()),
                row_values,
            );
            ((object_id, row_values), receiver)
        };

        let _ = receiver.await;
        Ok(serde_json::json!({
            "id": object_id.uuid().to_string(),
            "values": row_values,
        }))
    }

    #[napi(js_name = "updateDurable", ts_return_type = "Promise<void>")]
    pub async fn update_durable(
        &self,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        tier: String,
    ) -> napi::Result<()> {
        let persistence_tier = parse_tier(&tier)?;

        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let updates = convert_updates(partial_values);

        let receiver = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.update_persisted(oid, updates, None, persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Update failed: {e}")))?
        };

        let _ = receiver.await;
        Ok(())
    }

    #[napi(js_name = "updateDurableWithSession", ts_return_type = "Promise<void>")]
    pub async fn update_durable_with_session(
        &self,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        session_json: Option<String>,
        tier: String,
    ) -> napi::Result<()> {
        let persistence_tier = parse_tier(&tier)?;

        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let session = parse_session_json(session_json)?;

        let partial_values: HashMap<String, Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let updates = convert_updates(partial_values);

        let receiver = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.update_persisted(oid, updates, session.as_ref(), persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Update failed: {:?}", e)))?
        };

        let _ = receiver.await;
        Ok(())
    }

    #[napi(js_name = "deleteDurable", ts_return_type = "Promise<void>")]
    pub async fn delete_durable(&self, object_id: String, tier: String) -> napi::Result<()> {
        let persistence_tier = parse_tier(&tier)?;

        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let receiver = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.delete_persisted(oid, None, persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?
        };

        let _ = receiver.await;
        Ok(())
    }

    #[napi(js_name = "deleteDurableWithSession", ts_return_type = "Promise<void>")]
    pub async fn delete_durable_with_session(
        &self,
        object_id: String,
        session_json: Option<String>,
        tier: String,
    ) -> napi::Result<()> {
        let persistence_tier = parse_tier(&tier)?;

        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let session = parse_session_json(session_json)?;

        let receiver = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.delete_persisted(oid, session.as_ref(), persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?
        };

        let _ = receiver.await;
        Ok(())
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    #[napi(js_name = "onSyncMessageReceived")]
    pub fn on_sync_message_received(&self, message_json: String) -> napi::Result<()> {
        let payload: SyncPayload = serde_json::from_str(&message_json)
            .map_err(|e| napi::Error::from_reason(format!("Invalid sync message: {}", e)))?;

        let entry = InboxEntry {
            source: Source::Server(ServerId::new()),
            payload,
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.park_sync_message(entry);
        Ok(())
    }

    /// Called by JS when a sync message arrives from a client (not a server).
    #[napi(js_name = "onSyncMessageReceivedFromClient")]
    pub fn on_sync_message_received_from_client(
        &self,
        client_id: String,
        message_json: String,
    ) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&client_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let payload: SyncPayload = serde_json::from_str(&message_json)
            .map_err(|e| napi::Error::from_reason(format!("Invalid sync message: {}", e)))?;

        let entry = InboxEntry {
            source: Source::Client(cid),
            payload,
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.park_sync_message(entry);
        Ok(())
    }

    #[napi(js_name = "onSyncMessageToSend")]
    pub fn on_sync_message_to_send(
        &self,
        #[napi(ts_arg_type = "(...args: any[]) => any")] callback: ThreadsafeFunction<
            SyncCallbackParams,
        >,
    ) -> napi::Result<()> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.sync_sender().set_callback(callback);
        Ok(())
    }

    #[napi(js_name = "addServer")]
    pub fn add_server(&self) -> napi::Result<()> {
        let server_id = {
            let mut slot = self
                .upstream_server_id
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            if let Some(server_id) = *slot {
                server_id
            } else {
                let server_id = ServerId::new();
                *slot = Some(server_id);
                server_id
            }
        };
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        // Re-attach semantics: remove existing upstream edge then add again so
        // replay/full-sync runs on every successful reconnect.
        core.remove_server(server_id);
        core.add_server(server_id);
        Ok(())
    }

    #[napi(js_name = "removeServer")]
    pub fn remove_server(&self) -> napi::Result<()> {
        let Some(server_id) = *self
            .upstream_server_id
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
        else {
            return Ok(());
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.remove_server(server_id);
        Ok(())
    }

    #[napi(js_name = "addClient")]
    pub fn add_client(&self) -> napi::Result<String> {
        let client_id = ClientId::new();
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.add_client(client_id, None);
        Ok(client_id.0.to_string())
    }

    /// Set a client's role ("user", "admin", or "peer").
    #[napi(js_name = "setClientRole")]
    pub fn set_client_role(&self, client_id: String, role: String) -> napi::Result<()> {
        use jazz_tools::sync_manager::ClientRole;

        let uuid = uuid::Uuid::parse_str(&client_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let client_role = match role.as_str() {
            "user" => ClientRole::User,
            "admin" => ClientRole::Admin,
            "peer" => ClientRole::Peer,
            _ => {
                return Err(napi::Error::from_reason(format!(
                    "Invalid role '{}'. Must be 'user', 'admin', or 'peer'.",
                    role
                )));
            }
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.set_client_role_by_name(cid, client_role);
        Ok(())
    }

    // =========================================================================
    // Schema Access
    // =========================================================================

    #[napi(js_name = "getSchema", ts_return_type = "any")]
    pub fn get_schema(&self) -> napi::Result<serde_json::Value> {
        serde_json::to_value(&self.declared_schema)
            .map_err(|e| napi::Error::from_reason(format!("Schema serialization failed: {}", e)))
    }

    #[napi(js_name = "getSchemaHash")]
    pub fn get_schema_hash(&self) -> napi::Result<String> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let schema = core.current_schema();
        Ok(SchemaHash::compute(schema).to_string())
    }

    #[napi]
    pub fn flush(&self) -> napi::Result<()> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.storage().flush();
        Ok(())
    }

    /// Flush and close the underlying storage, releasing filesystem locks.
    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.storage().flush();
        core.storage()
            .close()
            .map_err(|e| napi::Error::from_reason(format!("Failed to close storage: {:?}", e)))?;
        Ok(())
    }
}

// ============================================================================
// Module-level utility functions
// ============================================================================

#[napi(js_name = "generateId")]
pub fn generate_id() -> String {
    generate_binding_id()
}

#[napi(js_name = "currentTimestamp")]
pub fn current_timestamp() -> i64 {
    current_timestamp_ms()
}

#[napi(js_name = "parseSchema", ts_return_type = "any")]
pub fn parse_schema_fn(json: String) -> napi::Result<serde_json::Value> {
    let schema: Schema = serde_json::from_str(&json)
        .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
    serde_json::to_value(&schema)
        .map_err(|e| napi::Error::from_reason(format!("Schema serialization failed: {}", e)))
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

    #[test]
    fn schema_json_roundtrip_preserves_enum_and_fk() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("files").column("name", ColumnType::Text))
            .table(
                TableSchema::builder("todos")
                    .column(
                        "status",
                        ColumnType::Enum {
                            variants: vec!["done".to_string(), "todo".to_string()],
                        },
                    )
                    .fk_column("image", "files"),
            )
            .build();

        let encoded = serde_json::to_string(&schema).expect("serialize schema");
        let decoded: Schema = serde_json::from_str(&encoded).expect("deserialize schema");

        let status = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("status")
            .unwrap();
        assert_eq!(
            status.column_type,
            ColumnType::Enum {
                variants: vec!["done".to_string(), "todo".to_string()]
            }
        );

        let image = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("image")
            .unwrap();
        assert_eq!(image.references, Some(TableName::new("files")));
    }

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

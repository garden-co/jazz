//! jazz-napi — Native Node.js bindings for Jazz.
//!
//! Provides `NapiRuntime` wrapping `RuntimeCore<SurrealKvStorage>` via napi-rs.
//! Exposed as the `jazz-napi` npm package for server-side TypeScript apps.
//!
//! # Architecture
//!
//! - `SurrealKvStorage` provides persistent on-disk storage
//! - `NapiScheduler` implements `Scheduler` using `ThreadsafeFunction` to schedule
//!   `batched_tick()` on the Node.js event loop (debounced)
//! - `NapiSyncSender` implements `SyncSender` bridging to a JS callback
//! - `NapiRuntime` wraps `Arc<Mutex<RuntimeCore<...>>>`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use napi::Env;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::encoding::decode_row;
use jazz_tools::query_manager::parse_query_json;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{Schema, SchemaHash, Value};
use jazz_tools::runtime_core::{
    RuntimeCore, Scheduler, SubscriptionDelta, SubscriptionHandle, SyncSender,
};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::{Storage, SurrealKvStorage};
use jazz_tools::sync_manager::QueryPropagation;
use jazz_tools::sync_manager::{
    ClientId, InboxEntry, OutboxEntry, PersistenceTier, ServerId, Source, SyncManager, SyncPayload,
};

fn convert_values(values: Vec<Value>) -> Vec<Value> {
    values
}

fn convert_updates(partial: HashMap<String, Value>) -> Vec<(String, Value)> {
    partial.into_iter().collect()
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
struct QueryExecutionOptionsWire {
    propagation: Option<String>,
}

fn parse_propagation(options_json: Option<String>) -> napi::Result<QueryPropagation> {
    let Some(raw) = options_json else {
        return Ok(QueryPropagation::Full);
    };

    let options: QueryExecutionOptionsWire = serde_json::from_str(&raw)
        .map_err(|e| napi::Error::from_reason(format!("Invalid query options JSON: {}", e)))?;

    match options.propagation.as_deref() {
        None | Some("full") => Ok(QueryPropagation::Full),
        Some("local-only") => Ok(QueryPropagation::LocalOnly),
        Some(other) => Err(napi::Error::from_reason(format!(
            "Invalid propagation '{}'. Must be 'full' or 'local-only'.",
            other
        ))),
    }
}

// ============================================================================
fn parse_tier(tier: &str) -> napi::Result<PersistenceTier> {
    match tier {
        "worker" => Ok(PersistenceTier::Worker),
        "edge" => Ok(PersistenceTier::EdgeServer),
        "core" => Ok(PersistenceTier::CoreServer),
        _ => Err(napi::Error::from_reason(format!(
            "Invalid tier '{}'. Must be 'worker', 'edge', or 'core'.",
            tier
        ))),
    }
}

fn parse_query(json: &str) -> napi::Result<Query> {
    parse_query_json(json).map_err(napi::Error::from_reason)
}

// ============================================================================
// NapiScheduler
// ============================================================================

type NapiCoreType = RuntimeCore<SurrealKvStorage, NapiScheduler, NapiSyncSender>;

/// Scheduler that schedules `batched_tick()` on the Node.js event loop via a
/// ThreadsafeFunction wrapping a noop JS function. The TSFN callback closure
/// does the actual work. Debounced: only one tick is pending at a time.
pub struct NapiScheduler {
    scheduled: Arc<AtomicBool>,
    core_ref: Weak<Mutex<NapiCoreType>>,
    tsfn: Option<ThreadsafeFunction<(), ErrorStrategy::CalleeHandled>>,
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

    fn set_tsfn(&mut self, tsfn: ThreadsafeFunction<(), ErrorStrategy::CalleeHandled>) {
        self.tsfn = Some(tsfn);
    }
}

impl Scheduler for NapiScheduler {
    fn schedule_batched_tick(&self) {
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            if let Some(ref tsfn) = self.tsfn {
                tsfn.call(Ok(()), ThreadsafeFunctionCallMode::NonBlocking);
            } else {
                self.scheduled.store(false, Ordering::SeqCst);
            }
        }
    }
}

// ============================================================================
// NapiSyncSender
// ============================================================================

pub struct NapiSyncSender {
    callback: Arc<Mutex<Option<ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>>>>,
}

impl NapiSyncSender {
    fn new() -> Self {
        Self {
            callback: Arc::new(Mutex::new(None)),
        }
    }

    fn set_callback(&self, tsfn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>) {
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
        let json = match serde_json::to_string(&message) {
            Ok(json) => json,
            Err(_) => return,
        };

        tsfn.call(Ok(json), ThreadsafeFunctionCallMode::NonBlocking);
    }
}

// ============================================================================
// NapiRuntime
// ============================================================================

#[napi]
pub struct NapiRuntime {
    core: Arc<Mutex<NapiCoreType>>,
    upstream_server_id: Mutex<Option<ServerId>>,
}

#[napi]
impl NapiRuntime {
    /// Create a new NapiRuntime with SurrealKV-backed persistent storage.
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
        // Parse schema
        let schema: Schema = serde_json::from_str(&schema_json)
            .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;

        // Parse optional tier
        let persistence_tier = tier.as_deref().map(parse_tier).transpose()?;

        // Create sync manager
        let mut sync_manager = SyncManager::new();
        if let Some(t) = persistence_tier {
            sync_manager = sync_manager.with_tier(t);
        }

        // Create schema manager
        let schema_manager = SchemaManager::new(
            sync_manager,
            schema,
            AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id)),
            &jazz_env,
            &user_branch,
        )
        .map_err(|e| {
            napi::Error::from_reason(format!("Failed to create SchemaManager: {:?}", e))
        })?;

        // Create SurrealKvStorage
        let cache_size = 64 * 1024 * 1024; // 64MB default
        let storage = SurrealKvStorage::open(&data_path, cache_size)
            .map_err(|e| napi::Error::from_reason(format!("Failed to open storage: {:?}", e)))?;

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

            // Create a noop JS function to wrap in a TSFN.
            // The TSFN callback closure does the real work (batched_tick).
            // The noop function receives the return value but ignores it.
            let noop_fn = env.create_function_from_closure("__groove_tick", |_ctx| Ok(()))?;

            let core_ref_for_tsfn = core_weak.clone();
            let flag_for_tsfn = scheduled_flag;

            let mut tsfn = env.create_threadsafe_function(
                &noop_fn,
                0, // max_queue_size: 0 = unlimited
                move |_ctx: napi::threadsafe_function::ThreadSafeCallContext<()>| {
                    // Reset flag first so new ticks can be scheduled
                    flag_for_tsfn.store(false, Ordering::SeqCst);
                    let Some(core_arc) = core_ref_for_tsfn.upgrade() else {
                        // Return empty vec — noop function doesn't use args
                        return Ok(Vec::<napi::JsUnknown>::new());
                    };
                    if let Ok(mut core) = core_arc.lock() {
                        core.batched_tick();
                    }
                    // Return empty vec — noop function doesn't use args
                    Ok(Vec::<napi::JsUnknown>::new())
                },
            )?;

            // Don't keep the Node.js event loop alive for the scheduler
            tsfn.unref(&env)?;

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
        })
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    #[napi]
    pub fn insert(
        &self,
        table: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
    ) -> napi::Result<String> {
        let js_values: Vec<Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let groove_values = convert_values(js_values);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let result = core
            .insert(&table, groove_values, None)
            .map_err(|e| napi::Error::from_reason(format!("Insert failed: {:?}", e)))?;

        Ok(result.uuid().to_string())
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

    // =========================================================================
    // Queries
    // =========================================================================

    #[napi(ts_return_type = "Promise<any>")]
    pub fn query(
        &self,
        env: Env,
        query_json: String,
        session_json: Option<String>,
        settled_tier: Option<String>,
        options_json: Option<String>,
    ) -> napi::Result<napi::JsObject> {
        let query = parse_query(&query_json)?;

        let session =
            if let Some(json) = session_json {
                Some(serde_json::from_str::<Session>(&json).map_err(|e| {
                    napi::Error::from_reason(format!("Invalid session JSON: {}", e))
                })?)
            } else {
                None
            };

        let tier = settled_tier.as_deref().map(parse_tier).transpose()?;
        let propagation = parse_propagation(options_json)?;

        let future = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.query_with_propagation(query, session, tier, propagation)
        };

        // Create a deferred/promise pair
        let (deferred, promise) = env.create_deferred()?;

        // Spawn a thread to block on the oneshot receiver
        std::thread::spawn(move || {
            let result = futures::executor::block_on(future);

            match result {
                Ok(rows) => {
                    let json_rows: Vec<serde_json::Value> = rows
                        .into_iter()
                        .map(|(id, values)| {
                            serde_json::json!({
                                "id": id.uuid().to_string(),
                                "values": values
                            })
                        })
                        .collect();

                    deferred.resolve(move |env| env.to_js_value(&json_rows));
                }
                Err(e) => {
                    deferred.reject(napi::Error::from_reason(format!("Query failed: {:?}", e)));
                }
            }
        });

        Ok(promise)
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    #[napi]
    pub fn subscribe(
        &self,
        query_json: String,
        #[napi(ts_arg_type = "(...args: any[]) => any")] on_update: napi::JsFunction,
        session_json: Option<String>,
        settled_tier: Option<String>,
        options_json: Option<String>,
    ) -> napi::Result<f64> {
        let query = parse_query(&query_json)?;

        let session =
            if let Some(json) = session_json {
                Some(serde_json::from_str::<Session>(&json).map_err(|e| {
                    napi::Error::from_reason(format!("Invalid session JSON: {}", e))
                })?)
            } else {
                None
            };

        let tier = settled_tier.as_deref().map(parse_tier).transpose()?;
        let propagation = parse_propagation(options_json)?;

        // Create a ThreadsafeFunction for the JS callback.
        // The closure converts our serde_json::Value into a JsUnknown to pass to JS.
        let tsfn: ThreadsafeFunction<serde_json::Value, ErrorStrategy::CalleeHandled> =
            on_update.create_threadsafe_function(0, |ctx| {
                let val = ctx.env.to_js_value(&ctx.value)?;
                Ok(vec![val])
            })?;

        let callback = move |delta: SubscriptionDelta| {
            let row_to_json = |row: &jazz_tools::query_manager::types::Row,
                               descriptor: &jazz_tools::query_manager::types::RowDescriptor|
             -> serde_json::Value {
                let values = decode_row(descriptor, &row.data)
                    .map(|vals| vals.into_iter().collect::<Vec<_>>())
                    .unwrap_or_default();
                serde_json::json!({
                    "id": row.id.uuid().to_string(),
                    "values": values
                })
            };

            let descriptor = &delta.descriptor;
            let delta_obj = delta
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
                .chain(delta.ordered_delta.updated.iter().map(|change| {
                    serde_json::json!({
                        "kind": 2,
                        "id": change.id.uuid().to_string(),
                        "index": change.new_index,
                        "row": change.row.as_ref().map(|row| row_to_json(row, descriptor))
                    })
                }))
                .chain(delta.ordered_delta.added.iter().map(|change| {
                    serde_json::json!({
                        "kind": 0,
                        "id": change.id.uuid().to_string(),
                        "index": change.index,
                        "row": row_to_json(&change.row, descriptor)
                    })
                }))
                .collect::<Vec<_>>();

            tsfn.call(
                Ok(serde_json::Value::Array(delta_obj)),
                ThreadsafeFunctionCallMode::NonBlocking,
            );
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let handle = core
            .subscribe_with_settled_tier_and_propagation(
                query,
                callback,
                session,
                tier,
                propagation,
            )
            .map_err(|e| napi::Error::from_reason(format!("Subscribe failed: {:?}", e)))?;

        Ok(handle.0 as f64)
    }

    #[napi]
    pub fn unsubscribe(&self, handle: f64) -> napi::Result<()> {
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.unsubscribe(SubscriptionHandle(handle as u64));
        Ok(())
    }

    // =========================================================================
    // Persisted CRUD Operations
    // =========================================================================

    #[napi(js_name = "insertWithAck", ts_return_type = "Promise<string>")]
    pub fn insert_with_ack(
        &self,
        env: Env,
        table: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        tier: String,
    ) -> napi::Result<napi::JsObject> {
        let persistence_tier = parse_tier(&tier)?;

        let js_values: Vec<Value> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let groove_values = convert_values(js_values);

        let (object_id, receiver) = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.insert_persisted(&table, groove_values, None, persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Insert failed: {:?}", e)))?
        };

        let id_str = object_id.uuid().to_string();
        let (deferred, promise) = env.create_deferred()?;
        std::thread::spawn(move || {
            let _ = futures::executor::block_on(receiver);
            deferred.resolve(move |env| env.create_string(&id_str));
        });

        Ok(promise)
    }

    #[napi(js_name = "updateWithAck", ts_return_type = "Promise<void>")]
    pub fn update_with_ack(
        &self,
        env: Env,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        tier: String,
    ) -> napi::Result<napi::JsObject> {
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
                .map_err(|e| napi::Error::from_reason(format!("Update failed: {:?}", e)))?
        };

        let (deferred, promise) = env.create_deferred()?;
        std::thread::spawn(move || {
            let _ = futures::executor::block_on(receiver);
            deferred.resolve(move |env| env.get_undefined());
        });

        Ok(promise)
    }

    #[napi(js_name = "deleteWithAck", ts_return_type = "Promise<void>")]
    pub fn delete_with_ack(
        &self,
        env: Env,
        object_id: String,
        tier: String,
    ) -> napi::Result<napi::JsObject> {
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

        let (deferred, promise) = env.create_deferred()?;
        std::thread::spawn(move || {
            let _ = futures::executor::block_on(receiver);
            deferred.resolve(move |env| env.get_undefined());
        });

        Ok(promise)
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
        #[napi(ts_arg_type = "(...args: any[]) => any")] callback: napi::JsFunction,
    ) -> napi::Result<()> {
        let tsfn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled> = callback
            .create_threadsafe_function(0, |ctx| {
                let val = ctx.env.create_string_from_std(ctx.value)?;
                Ok(vec![val])
            })?;

        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.sync_sender().set_callback(tsfn);
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
    pub fn get_schema(&self, env: Env) -> napi::Result<napi::JsUnknown> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let schema = core.current_schema();
        env.to_js_value(schema)
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
    ObjectId::new().uuid().to_string()
}

#[napi(js_name = "currentTimestamp")]
pub fn current_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[napi(js_name = "parseSchema", ts_return_type = "any")]
pub fn parse_schema_fn(env: Env, json: String) -> napi::Result<napi::JsUnknown> {
    let schema: Schema = serde_json::from_str(&json)
        .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
    env.to_js_value(&schema)
}

#[cfg(test)]
mod tests {
    use jazz_tools::query_manager::types::{
        ColumnType, Schema, SchemaBuilder, TableName, TableSchema,
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
}

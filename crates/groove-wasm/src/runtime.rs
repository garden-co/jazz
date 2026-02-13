//! WasmRuntime - Main entry point for JavaScript applications.
//!
//! Provides the core Jazz database functionality exposed to JavaScript:
//! - CRUD operations (insert, query, update, delete)
//! - Reactive subscriptions with callback-based updates
//! - Sync message handling for server communication
//!
//! # Architecture
//!
//! - `JazzLsmStorage` provides synchronous storage (from groove::storage)
//! - `WasmScheduler` implements `Scheduler` using `spawn_local` (debounced)
//! - `JsSyncSender` implements `SyncSender` bridging to a JS callback
//! - `WasmRuntime` wraps `Rc<RefCell<RuntimeCore<...>>>`

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::Once;

use js_sys::Function;
use serde::Serialize;
use tracing::{debug_span, info, info_span};
use wasm_bindgen::prelude::*;

/// Initialize wasm-tracing exactly once (idempotent across multiple WasmRuntime instances).
fn init_tracing() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let config = wasm_tracing::WasmLayerConfig::new()
            .with_max_level(tracing::Level::TRACE)
            .with_console_group_spans();
        let _ = wasm_tracing::set_as_global_default_with_config(config);
    });
}

use groove::object::ObjectId;
#[cfg(target_arch = "wasm32")]
use groove::query_manager::encoding::decode_row;
use groove::query_manager::session::Session;
#[cfg(target_arch = "wasm32")]
use groove::query_manager::types::{Row, RowDescriptor};
use groove::query_manager::types::{Schema, Value};
use groove::runtime_core::{RuntimeCore, Scheduler, SyncSender};
#[cfg(target_arch = "wasm32")]
use groove::runtime_core::{SubscriptionDelta, SubscriptionHandle};
use groove::schema_manager::{AppId, SchemaManager};
use groove::storage::JazzLsmStorage;
use groove::sync_manager::{
    ClientId, InboxEntry, OutboxEntry, PersistenceTier, ServerId, Source, SyncManager, SyncPayload,
};

use crate::query::parse_query;
use crate::types::{WasmSchema, WasmValue};

/// Parse a persistence tier string from JS.
fn parse_tier(tier: &str) -> Result<PersistenceTier, JsError> {
    match tier {
        "worker" => Ok(PersistenceTier::Worker),
        "edge" => Ok(PersistenceTier::EdgeServer),
        "core" => Ok(PersistenceTier::CoreServer),
        _ => Err(JsError::new(&format!(
            "Invalid tier '{}'. Must be 'worker', 'edge', or 'core'.",
            tier
        ))),
    }
}

// ============================================================================
// Type alias
// ============================================================================

/// Concrete RuntimeCore type for WASM.
type WasmCoreType = RuntimeCore<JazzLsmStorage, WasmScheduler, JsSyncSender>;

// ============================================================================
// WasmScheduler
// ============================================================================

/// Scheduler implementation for WASM.
///
/// Uses `wasm_bindgen_futures::spawn_local` to schedule a batched tick.
/// Debounced: only one task is scheduled at a time.
pub struct WasmScheduler {
    /// Debounce flag for scheduled ticks.
    scheduled: Rc<RefCell<bool>>,
    /// Weak reference back to RuntimeCore for spawned tasks.
    core_ref: Weak<RefCell<WasmCoreType>>,
}

impl WasmScheduler {
    fn new() -> Self {
        Self {
            scheduled: Rc::new(RefCell::new(false)),
            core_ref: Weak::new(),
        }
    }

    fn set_core_ref(&mut self, core_ref: Weak<RefCell<WasmCoreType>>) {
        self.core_ref = core_ref;
    }
}

impl Scheduler for WasmScheduler {
    fn schedule_batched_tick(&self) {
        let mut scheduled = self.scheduled.borrow_mut();
        if !*scheduled {
            *scheduled = true;

            let core_ref = self.core_ref.clone();
            let flag = self.scheduled.clone();

            wasm_bindgen_futures::spawn_local(async move {
                *flag.borrow_mut() = false;
                if let Some(core_rc) = core_ref.upgrade() {
                    core_rc.borrow_mut().batched_tick();
                }
            });
        }
    }
}

// ============================================================================
// JsSyncSender
// ============================================================================

/// SyncSender implementation bridging to a JS callback.
///
/// The callback is set lazily via `on_sync_message_to_send()`.
pub struct JsSyncSender {
    callback: RefCell<Option<Function>>,
}

impl JsSyncSender {
    fn new() -> Self {
        Self {
            callback: RefCell::new(None),
        }
    }

    fn set_callback(&self, callback: Function) {
        *self.callback.borrow_mut() = Some(callback);
    }
}

impl SyncSender for JsSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        if let Some(ref callback) = *self.callback.borrow() {
            if let Ok(json) = serde_json::to_string(&message) {
                let js_value = JsValue::from_str(&json);
                let _ = callback.call1(&JsValue::NULL, &js_value);
            }
        }
    }
}

// ============================================================================
// WasmRuntime
// ============================================================================

/// Main runtime for JavaScript applications.
///
/// Wraps `Rc<RefCell<RuntimeCore<JazzLsmStorage, WasmScheduler, JsSyncSender>>>`.
/// All methods borrow the core, call RuntimeCore, and return.
/// Async scheduling happens via WasmScheduler.schedule_batched_tick().
#[wasm_bindgen]
pub struct WasmRuntime {
    core: Rc<RefCell<WasmCoreType>>,
    /// Label for tracing (e.g. "worker", "edge", or "client").
    tier_label: &'static str,
}

#[wasm_bindgen]
impl WasmRuntime {
    /// Create a new WasmRuntime.
    ///
    /// Storage is synchronous (in-memory via JazzLsmStorage).
    ///
    /// # Arguments
    /// * `schema_json` - JSON-encoded schema definition
    /// * `app_id` - Application identifier
    /// * `env` - Environment (e.g., "dev", "prod")
    /// * `user_branch` - User's branch name (e.g., "main")
    /// * `tier` - Optional persistence tier ("worker", "edge", "core").
    ///            Set for server nodes to enable ack emission.
    #[wasm_bindgen(constructor)]
    pub fn new(
        schema_json: &str,
        app_id: &str,
        env: &str,
        user_branch: &str,
        tier: Option<String>,
    ) -> Result<WasmRuntime, JsError> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();
        init_tracing();

        let tier_label = match tier.as_deref() {
            Some("worker") => "worker",
            Some("edge") => "edge",
            Some("core") => "core",
            _ => "client",
        };
        let _span = info_span!(
            "WasmRuntime::new",
            tier = tier_label,
            app_id,
            env,
            user_branch
        )
        .entered();
        info!("creating in-memory runtime");

        // Parse schema
        let wasm_schema: WasmSchema = serde_json::from_str(schema_json)
            .map_err(|e| JsError::new(&format!("Invalid schema JSON: {}", e)))?;

        let schema: Schema = wasm_schema
            .try_into()
            .map_err(|e: String| JsError::new(&e))?;

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
            AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id)),
            env,
            user_branch,
        )
        .map_err(|e| JsError::new(&format!("Failed to create SchemaManager: {:?}", e)))?;

        // Create components
        const DEFAULT_CACHE_SIZE: usize = 32 * 1024 * 1024; // 32MB
        let storage = JazzLsmStorage::memory(DEFAULT_CACHE_SIZE)
            .map_err(|e| JsError::new(&format!("Storage init: {:?}", e)))?;
        let scheduler = WasmScheduler::new();
        let sync_sender = JsSyncSender::new();

        // Create RuntimeCore
        let mut core = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);
        core.set_tier_label(tier_label);

        // Wrap in Rc<RefCell>
        let core_rc = Rc::new(RefCell::new(core));

        // Set the core_ref on the Scheduler
        {
            let mut core_guard = core_rc.borrow_mut();
            core_guard
                .scheduler_mut()
                .set_core_ref(Rc::downgrade(&core_rc));
        }

        // Persist schema to catalogue for server sync
        core_rc.borrow_mut().persist_schema();

        Ok(WasmRuntime {
            core: core_rc,
            tier_label,
        })
    }

    /// Called by JS when a sync message arrives from the server.
    ///
    /// # Arguments
    /// * `message_json` - JSON-encoded SyncPayload
    #[wasm_bindgen(js_name = onSyncMessageReceived)]
    pub fn on_sync_message_received(&self, message_json: &str) -> Result<(), JsError> {
        let _span = debug_span!("wasm::onSyncMessageReceived", tier = self.tier_label).entered();
        let payload: SyncPayload = serde_json::from_str(message_json)
            .map_err(|e| JsError::new(&format!("Invalid sync message: {}", e)))?;

        let entry = InboxEntry {
            source: Source::Server(ServerId::new()),
            payload,
        };

        self.core.borrow_mut().park_sync_message(entry);
        Ok(())
    }

    /// Called by JS when a sync message arrives from a client (not a server).
    ///
    /// # Arguments
    /// * `client_id` - UUID string of the sending client
    /// * `message_json` - JSON-encoded SyncPayload
    #[wasm_bindgen(js_name = onSyncMessageReceivedFromClient)]
    pub fn on_sync_message_received_from_client(
        &self,
        client_id: &str,
        message_json: &str,
    ) -> Result<(), JsError> {
        let _span = debug_span!(
            "wasm::onSyncMessageReceivedFromClient",
            tier = self.tier_label,
            client_id
        )
        .entered();
        let uuid = uuid::Uuid::parse_str(client_id)
            .map_err(|e| JsError::new(&format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let payload: SyncPayload = serde_json::from_str(message_json)
            .map_err(|e| JsError::new(&format!("Invalid sync message: {}", e)))?;

        let entry = InboxEntry {
            source: Source::Client(cid),
            payload,
        };

        self.core.borrow_mut().park_sync_message(entry);
        Ok(())
    }

    /// Register a callback for outgoing sync messages.
    #[wasm_bindgen(js_name = onSyncMessageToSend)]
    pub fn on_sync_message_to_send(&self, callback: Function) {
        self.core.borrow().sync_sender().set_callback(callback);
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    /// Insert a row into a table.
    ///
    /// # Returns
    /// The new row's ObjectId as a UUID string.
    #[wasm_bindgen]
    pub fn insert(&self, table: &str, values: JsValue) -> Result<String, JsError> {
        let _span = debug_span!("wasm::insert", tier = self.tier_label, table).entered();
        let wasm_values: Vec<WasmValue> = serde_wasm_bindgen::from_value(values)?;
        let groove_values: Vec<Value> = wasm_values
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .map_err(|e: String| JsError::new(&e))?;

        let mut core = self.core.borrow_mut();
        let result = core
            .insert(table, groove_values, None)
            .map_err(|e| JsError::new(&format!("Insert failed: {:?}", e)))?;

        Ok(result.uuid().to_string())
    }

    /// Execute a query and return results as a Promise.
    ///
    /// Optional `settled_tier` holds delivery until the tier confirms.
    #[wasm_bindgen]
    pub fn query(
        &self,
        query_json: &str,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> Result<js_sys::Promise, JsError> {
        let _span = debug_span!("wasm::query", tier = self.tier_label).entered();
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;

        let session = if let Some(json) = session_json {
            Some(
                serde_json::from_str::<Session>(&json)
                    .map_err(|e| JsError::new(&format!("Invalid session JSON: {}", e)))?,
            )
        } else {
            None
        };

        let tier = settled_tier.as_deref().map(parse_tier).transpose()?;

        let future = {
            let mut core = self.core.borrow_mut();
            core.query(query, session, tier)
        };

        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let results = future
                .await
                .map_err(|e| JsValue::from_str(&format!("Query failed: {:?}", e)))?;

            let wasm_results: Vec<_> = results
                .into_iter()
                .map(|(id, values)| {
                    let wasm_values: Vec<WasmValue> = values.into_iter().map(Into::into).collect();
                    serde_json::json!({
                        "id": id.uuid().to_string(),
                        "values": wasm_values
                    })
                })
                .collect();

            let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
            wasm_results
                .serialize(&serializer)
                .map_err(|e| JsValue::from_str(&format!("Serialization failed: {:?}", e)))
        });

        Ok(promise)
    }

    /// Update a row by ObjectId.
    #[wasm_bindgen]
    pub fn update(&self, object_id: &str, values: JsValue) -> Result<(), JsError> {
        let _span = debug_span!("wasm::update", tier = self.tier_label, object_id).entered();
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, WasmValue> = serde_wasm_bindgen::from_value(values)?;

        let updates: Vec<(String, Value)> = partial_values
            .into_iter()
            .map(|(k, v)| {
                let groove_value: Value = v.try_into()?;
                Ok((k, groove_value))
            })
            .collect::<Result<_, String>>()
            .map_err(|e: String| JsError::new(&e))?;

        let mut core = self.core.borrow_mut();
        core.update(oid, updates, None)
            .map_err(|e| JsError::new(&format!("Update failed: {:?}", e)))?;

        Ok(())
    }

    /// Delete a row by ObjectId.
    #[wasm_bindgen]
    pub fn delete(&self, object_id: &str) -> Result<(), JsError> {
        let _span = debug_span!("wasm::delete", tier = self.tier_label, object_id).entered();
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let mut core = self.core.borrow_mut();
        core.delete(oid, None)
            .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Persisted CRUD Operations
    // =========================================================================

    /// Insert a row and return a Promise that resolves when the tier acks.
    ///
    /// `tier` must be one of: "worker", "edge", "core".
    #[wasm_bindgen(js_name = insertPersisted)]
    pub fn insert_persisted(
        &self,
        table: &str,
        values: JsValue,
        tier: &str,
    ) -> Result<js_sys::Promise, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let wasm_values: Vec<WasmValue> = serde_wasm_bindgen::from_value(values)?;
        let groove_values: Vec<Value> = wasm_values
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .map_err(|e: String| JsError::new(&e))?;

        let (object_id, receiver) = {
            let mut core = self.core.borrow_mut();
            core.insert_persisted(table, groove_values, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Insert failed: {:?}", e)))?
        };

        let id_str = object_id.uuid().to_string();
        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let _ = receiver.await;
            Ok(JsValue::from_str(&id_str))
        });

        Ok(promise)
    }

    /// Update a row and return a Promise that resolves when the tier acks.
    #[wasm_bindgen(js_name = updatePersisted)]
    pub fn update_persisted(
        &self,
        object_id: &str,
        values: JsValue,
        tier: &str,
    ) -> Result<js_sys::Promise, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, WasmValue> = serde_wasm_bindgen::from_value(values)?;
        let updates: Vec<(String, Value)> = partial_values
            .into_iter()
            .map(|(k, v)| {
                let groove_value: Value = v.try_into()?;
                Ok((k, groove_value))
            })
            .collect::<Result<_, String>>()
            .map_err(|e: String| JsError::new(&e))?;

        let receiver = {
            let mut core = self.core.borrow_mut();
            core.update_persisted(oid, updates, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Update failed: {:?}", e)))?
        };

        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let _ = receiver.await;
            Ok(JsValue::undefined())
        });

        Ok(promise)
    }

    /// Delete a row and return a Promise that resolves when the tier acks.
    #[wasm_bindgen(js_name = deletePersisted)]
    pub fn delete_persisted(
        &self,
        object_id: &str,
        tier: &str,
    ) -> Result<js_sys::Promise, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let receiver = {
            let mut core = self.core.borrow_mut();
            core.delete_persisted(oid, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?
        };

        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let _ = receiver.await;
            Ok(JsValue::undefined())
        });

        Ok(promise)
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
    ///
    /// # Returns
    /// Subscription handle (f64) for later unsubscription.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn subscribe(
        &self,
        query_json: &str,
        on_update: Function,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> Result<f64, JsError> {
        let _span = debug_span!("wasm::subscribe", tier = self.tier_label).entered();
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;

        let session = if let Some(json) = session_json {
            Some(
                serde_json::from_str::<Session>(&json)
                    .map_err(|e| JsError::new(&format!("Invalid session JSON: {}", e)))?,
            )
        } else {
            None
        };

        let tier = settled_tier.as_deref().map(parse_tier).transpose()?;

        let callback = move |delta: SubscriptionDelta| {
            let row_to_json = |row: &Row, descriptor: &RowDescriptor| -> serde_json::Value {
                let values = decode_row(descriptor, &row.data)
                    .map(|vals| vals.into_iter().map(WasmValue::from).collect::<Vec<_>>())
                    .unwrap_or_default();
                serde_json::json!({
                    "id": row.id.uuid().to_string(),
                    "values": values
                })
            };

            let descriptor = &delta.descriptor;

            let delta_json = serde_json::json!({
                "added": delta.delta.added.iter()
                    .map(|row| row_to_json(row, descriptor))
                    .collect::<Vec<_>>(),
                "removed": delta.delta.removed.iter()
                    .map(|row| row_to_json(row, descriptor))
                    .collect::<Vec<_>>(),
                "updated": delta.delta.updated.iter()
                    .map(|(old, new)| [row_to_json(old, descriptor), row_to_json(new, descriptor)])
                    .collect::<Vec<_>>()
            });

            if let Ok(json_str) = serde_json::to_string(&delta_json) {
                let _ = on_update.call1(&JsValue::NULL, &JsValue::from_str(&json_str));
            }
        };

        let handle = self
            .core
            .borrow_mut()
            .subscribe_with_settled_tier(query, callback, session, tier)
            .map_err(|e| JsError::new(&format!("Subscribe failed: {:?}", e)))?;

        Ok(handle.0 as f64)
    }

    /// Unsubscribe from a query.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn unsubscribe(&self, handle: f64) {
        self.core
            .borrow_mut()
            .unsubscribe(SubscriptionHandle(handle as u64));
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    /// Add a server connection.
    ///
    /// After adding the server, immediately flushes the outbox so that
    /// catalogue sync messages (from queue_full_sync_to_server) are sent
    /// before the call returns, rather than being deferred to a microtask.
    #[wasm_bindgen(js_name = addServer)]
    pub fn add_server(&self) {
        let _span = info_span!("wasm::addServer", tier = self.tier_label).entered();
        let server_id = ServerId::new();
        let mut core = self.core.borrow_mut();
        core.add_server(server_id);
        core.batched_tick();
    }

    /// Add a client connection (for server-side use in tests).
    #[wasm_bindgen(js_name = addClient)]
    pub fn add_client(&self) -> String {
        let _span = info_span!("wasm::addClient", tier = self.tier_label).entered();
        let client_id = ClientId::new();
        info!(%client_id, "generated client id");
        let mut core = self.core.borrow_mut();
        core.add_client(client_id, None);
        client_id.0.to_string()
    }

    /// Set a client's role.
    ///
    /// # Arguments
    /// * `client_id` - UUID string of the client
    /// * `role` - One of "user", "admin", "peer"
    #[wasm_bindgen(js_name = setClientRole)]
    pub fn set_client_role(&self, client_id: &str, role: &str) -> Result<(), JsError> {
        use groove::sync_manager::ClientRole;

        let uuid = uuid::Uuid::parse_str(client_id)
            .map_err(|e| JsError::new(&format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let client_role = match role {
            "user" => ClientRole::User,
            "admin" => ClientRole::Admin,
            "peer" => ClientRole::Peer,
            _ => {
                return Err(JsError::new(&format!(
                    "Invalid role '{}'. Must be 'user', 'admin', or 'peer'.",
                    role
                )));
            }
        };

        self.core
            .borrow_mut()
            .set_client_role_by_name(cid, client_role);
        Ok(())
    }

    // =========================================================================
    // Schema Access
    // =========================================================================

    /// Get the current schema as JSON.
    #[wasm_bindgen(js_name = getSchema)]
    pub fn get_schema(&self) -> Result<JsValue, JsError> {
        let core = self.core.borrow();
        let schema = core.current_schema();
        let wasm_schema = WasmSchema::from(schema);
        Ok(serde_wasm_bindgen::to_value(&wasm_schema)?)
    }

    /// Flush all data to persistent storage (snapshot).
    #[wasm_bindgen]
    pub fn flush(&self) {
        let _span = debug_span!("wasm::flush", tier = self.tier_label).entered();
        self.core.borrow().flush_storage();
    }

    /// Flush only the WAL buffer to OPFS (not the snapshot).
    #[wasm_bindgen(js_name = flushWal)]
    pub fn flush_wal(&self) {
        let _span = debug_span!("wasm::flushWal", tier = self.tier_label).entered();
        self.core.borrow().flush_wal();
    }

    /// Create a persistent WasmRuntime backed by OPFS.
    ///
    /// Opens one OPFS container file: `{db_name}.jazzlsmfs`.
    /// Handles fresh start and crash recovery automatically.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openPersistent)]
    pub async fn open_persistent(
        schema_json: &str,
        app_id: &str,
        env: &str,
        user_branch: &str,
        db_name: &str,
        tier: Option<String>,
    ) -> Result<WasmRuntime, JsError> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();
        init_tracing();

        let tier_label = match tier.as_deref() {
            Some("worker") => "worker",
            Some("edge") => "edge",
            Some("core") => "core",
            _ => "client",
        };
        let _span = info_span!(
            "WasmRuntime::openPersistent",
            tier = tier_label,
            app_id,
            env,
            user_branch,
            db_name
        )
        .entered();
        info!("opening persistent OPFS runtime");

        // Parse schema
        let wasm_schema: WasmSchema = serde_json::from_str(schema_json)
            .map_err(|e| JsError::new(&format!("Invalid schema JSON: {}", e)))?;

        let schema: Schema = wasm_schema
            .try_into()
            .map_err(|e: String| JsError::new(&e))?;

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
            AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id)),
            env,
            user_branch,
        )
        .map_err(|e| JsError::new(&format!("Failed to create SchemaManager: {:?}", e)))?;

        const DEFAULT_CACHE_SIZE: usize = 32 * 1024 * 1024;
        let storage = JazzLsmStorage::with_opfs(db_name, DEFAULT_CACHE_SIZE)
            .await
            .map_err(|e| JsError::new(&format!("Storage: {:?}", e)))?;

        let scheduler = WasmScheduler::new();
        let sync_sender = JsSyncSender::new();

        // Create RuntimeCore
        let mut core = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);
        core.set_tier_label(tier_label);

        // Wrap in Rc<RefCell>
        let core_rc = Rc::new(RefCell::new(core));

        // Set the core_ref on the Scheduler
        {
            let mut core_guard = core_rc.borrow_mut();
            core_guard
                .scheduler_mut()
                .set_core_ref(Rc::downgrade(&core_rc));
        }

        // Persist schema to catalogue for server sync
        core_rc.borrow_mut().persist_schema();

        Ok(WasmRuntime {
            core: core_rc,
            tier_label,
        })
    }
}

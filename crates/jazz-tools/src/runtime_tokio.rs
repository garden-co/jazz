//! Tokio runtime adapter for Jazz.
//!
//! Provides `TokioRuntime<S>` - a thin wrapper around
//! `RuntimeCore<S, TokioScheduler<S>, CallbackSyncSender>`
//! that handles async scheduling via `tokio::spawn`.
//!
//! # Architecture
//!
//! - `S: Storage + Send + 'static` provides synchronous storage
//! - `TokioScheduler<S>` implements `Scheduler` using tokio::spawn for batched ticks
//! - `CallbackSyncSender` implements `SyncSender` with a user-provided callback
//! - `TokioRuntime<S>` wraps `Arc<Mutex<RuntimeCore<...>>>`
//! - Methods grab the lock, call RuntimeCore, and return

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures::channel::oneshot;

use crate::batch_fate::LocalBatchRecord;
use crate::object::ObjectId;
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{Schema, SchemaHash, Value};
use crate::row_histories::BatchId;
pub use crate::runtime_core::SubscriptionHandle;
use crate::runtime_core::{
    QueryFuture, ReadDurabilityOptions, RuntimeCore, RuntimeError as CoreRuntimeError, Scheduler,
    SubscriptionDelta, SyncSender,
};
use crate::schema_manager::manager::PermissionsHeadSummary;
use crate::schema_manager::{Lens, QuerySchemaContext, SchemaManager};
use crate::storage::Storage;
use crate::sync_manager::{ClientId, InboxEntry, OutboxEntry, QueryPropagation, ServerId};

// ============================================================================
// TokioScheduler
// ============================================================================

/// Type alias for the concrete RuntimeCore used by TokioRuntime.
type TokioCoreType<S> = RuntimeCore<S, TokioScheduler<S>, CallbackSyncSender>;

/// Scheduler implementation for Tokio.
///
/// Spawns a tokio task to call `batched_tick()` on the RuntimeCore.
/// Debounced: only one task is scheduled at a time.
pub struct TokioScheduler<S: Storage + Send + 'static> {
    /// Debounce flag for scheduled ticks.
    scheduled: Arc<AtomicBool>,
    /// Weak reference back to RuntimeCore for spawned tasks.
    core_ref: Weak<Mutex<TokioCoreType<S>>>,
}

impl<S: Storage + Send + 'static> TokioScheduler<S> {
    /// Create a new TokioScheduler.
    ///
    /// Note: `core_ref` starts as empty and is set after RuntimeCore is created.
    fn new() -> Self {
        Self {
            scheduled: Arc::new(AtomicBool::new(false)),
            core_ref: Weak::new(),
        }
    }

    /// Set the core reference (called after RuntimeCore is wrapped in Arc<Mutex>).
    fn set_core_ref(&mut self, core_ref: Weak<Mutex<TokioCoreType<S>>>) {
        self.core_ref = core_ref;
    }

    /// Check if a batched_tick is currently scheduled.
    pub fn is_scheduled(&self) -> bool {
        self.scheduled.load(Ordering::SeqCst)
    }
}

impl<S: Storage + Send + 'static> Scheduler for TokioScheduler<S> {
    fn schedule_batched_tick(&self) {
        // Debounce: only schedule if not already scheduled
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            let core_ref = self.core_ref.clone();
            let flag = self.scheduled.clone();

            tokio::spawn(async move {
                // Call batched_tick on the core
                if let Some(core_arc) = core_ref.upgrade()
                    && let Ok(mut core) = core_arc.lock()
                {
                    core.batched_tick();
                }

                // Clear the scheduled flag AFTER tick completes
                flag.store(false, Ordering::SeqCst);
            });
        }
    }
}

// ============================================================================
// CallbackSyncSender
// ============================================================================

/// SyncSender implementation using a callback.
pub struct CallbackSyncSender {
    callback: Arc<dyn Fn(OutboxEntry) + Send + Sync>,
}

impl CallbackSyncSender {
    fn new<F>(callback: F) -> Self
    where
        F: Fn(OutboxEntry) + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(callback),
        }
    }
}

impl SyncSender for CallbackSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        (self.callback)(message);
    }
}

// ============================================================================
// Errors
// ============================================================================

/// Errors from runtime operations.
#[derive(Debug, Clone)]
pub enum RuntimeError {
    QueryError(String),
    WriteError(String),
    NotFound,
    LockError,
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::QueryError(s) => write!(f, "Query error: {}", s),
            RuntimeError::WriteError(s) => write!(f, "Write error: {}", s),
            RuntimeError::NotFound => write!(f, "Not found"),
            RuntimeError::LockError => write!(f, "Lock error"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<CoreRuntimeError> for RuntimeError {
    fn from(e: CoreRuntimeError) -> Self {
        match e {
            CoreRuntimeError::QueryError(s) => RuntimeError::QueryError(s),
            CoreRuntimeError::WriteError(s) => RuntimeError::WriteError(s),
            CoreRuntimeError::NotFound => RuntimeError::NotFound,
        }
    }
}

// ============================================================================
// TokioRuntime
// ============================================================================

/// Tokio runtime for Jazz, generic over storage backend.
///
/// Thin wrapper around `Arc<Mutex<RuntimeCore<S, TokioScheduler<S>, CallbackSyncSender>>>`.
/// All methods grab the lock, call RuntimeCore, and return.
/// Async scheduling happens via TokioScheduler.schedule_batched_tick().
pub struct TokioRuntime<S: Storage + Send + 'static> {
    core: Arc<Mutex<TokioCoreType<S>>>,
}

// Manual Clone impl — only needs Arc::clone, not S: Clone
impl<S: Storage + Send + 'static> Clone for TokioRuntime<S> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<S: Storage + Send + 'static> TokioRuntime<S> {
    /// Create a new TokioRuntime with the given storage backend.
    ///
    /// # Arguments
    /// - `schema_manager` - The SchemaManager to wrap
    /// - `storage` - The storage backend (e.g., MemoryStorage, FjallStorage)
    /// - `sync_callback` - Called when sync messages need to be sent
    pub fn new<F>(schema_manager: SchemaManager, storage: S, sync_callback: F) -> Self
    where
        F: Fn(OutboxEntry) + Send + Sync + 'static,
    {
        let scheduler = TokioScheduler::new();
        let sync_sender = CallbackSyncSender::new(sync_callback);

        // Create RuntimeCore
        let core = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);

        // Wrap in Arc<Mutex>
        let core_arc = Arc::new(Mutex::new(core));

        // Set the core_ref on the Scheduler
        {
            let mut core_guard = core_arc.lock().unwrap();
            core_guard
                .scheduler_mut()
                .set_core_ref(Arc::downgrade(&core_arc));
        }

        Self { core: core_arc }
    }

    /// Persist the current schema to the catalogue for server sync.
    pub fn persist_schema(&self) -> Result<ObjectId, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.persist_schema())
    }

    /// Publish any schema object to the local catalogue and in-memory schema manager.
    pub fn publish_schema(&self, schema: Schema) -> Result<ObjectId, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.publish_schema(schema))
    }

    pub fn publish_permissions_bundle(
        &self,
        schema_hash: crate::query_manager::types::SchemaHash,
        permissions: std::collections::HashMap<
            crate::query_manager::types::TableName,
            crate::query_manager::types::TablePolicies,
        >,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.publish_permissions_bundle(schema_hash, permissions, expected_parent_bundle_object_id)
            .map_err(|error| RuntimeError::WriteError(error.to_string()))
    }

    pub fn current_permissions_head(&self) -> Result<Option<PermissionsHeadSummary>, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.schema_manager().current_permissions_head())
    }

    /// Publish a reviewed lens edge to the local catalogue and active schema manager.
    pub fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.publish_lens(lens)?)
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    /// Insert a row into a table.
    pub fn insert_with_write_context(
        &self,
        table: &str,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
    ) -> Result<(ObjectId, Vec<Value>), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        let result = core.insert(table, values, write_context)?;
        Ok(result)
    }

    /// Insert a row into a table.
    pub fn insert(
        &self,
        table: &str,
        values: HashMap<String, Value>,
        session: Option<&Session>,
    ) -> Result<(ObjectId, Vec<Value>), RuntimeError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.insert_with_write_context(table, values, owned.as_ref())
    }

    /// Update a row (partial update by column name).
    pub fn update_with_write_context(
        &self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        write_context: Option<&WriteContext>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.update(object_id, values, write_context)?;
        Ok(())
    }

    /// Update a row (partial update by column name).
    pub fn update(
        &self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.update_with_write_context(object_id, values, owned.as_ref())
    }

    /// Delete a row.
    pub fn delete_with_write_context(
        &self,
        object_id: ObjectId,
        write_context: Option<&WriteContext>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.delete(object_id, write_context)?;
        Ok(())
    }

    /// Delete a row.
    pub fn delete(
        &self,
        object_id: ObjectId,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.delete_with_write_context(object_id, owned.as_ref())
    }

    /// Insert a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn insert_persisted_with_write_context(
        &self,
        table: &str,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
        tier: crate::sync_manager::DurabilityTier,
    ) -> Result<((ObjectId, Vec<Value>), BatchId, oneshot::Receiver<()>), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.insert_persisted_with_batch_id(table, values, write_context, tier)?)
    }

    /// Insert a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn insert_persisted(
        &self,
        table: &str,
        values: HashMap<String, Value>,
        session: Option<&Session>,
        tier: crate::sync_manager::DurabilityTier,
    ) -> Result<((ObjectId, Vec<Value>), BatchId, oneshot::Receiver<()>), RuntimeError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.insert_persisted_with_write_context(table, values, owned.as_ref(), tier)
    }

    /// Update a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn update_persisted_with_write_context(
        &self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        write_context: Option<&WriteContext>,
        tier: crate::sync_manager::DurabilityTier,
    ) -> Result<(BatchId, oneshot::Receiver<()>), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.update_persisted_with_batch_id(object_id, values, write_context, tier)?)
    }

    /// Update a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn update_persisted(
        &self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
        tier: crate::sync_manager::DurabilityTier,
    ) -> Result<(BatchId, oneshot::Receiver<()>), RuntimeError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.update_persisted_with_write_context(object_id, values, owned.as_ref(), tier)
    }

    /// Delete a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn delete_persisted_with_write_context(
        &self,
        object_id: ObjectId,
        write_context: Option<&WriteContext>,
        tier: crate::sync_manager::DurabilityTier,
    ) -> Result<(BatchId, oneshot::Receiver<()>), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.delete_persisted_with_batch_id(object_id, write_context, tier)?)
    }

    /// Delete a row and return the logical batch id plus a receiver that
    /// resolves when the requested persistence tier (or higher) acknowledges.
    pub fn delete_persisted(
        &self,
        object_id: ObjectId,
        session: Option<&Session>,
        tier: crate::sync_manager::DurabilityTier,
    ) -> Result<(BatchId, oneshot::Receiver<()>), RuntimeError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.delete_persisted_with_write_context(object_id, owned.as_ref(), tier)
    }

    /// Flush pending operations to storage.
    ///
    /// Call this after CRUD operations if you need to ensure data is persisted
    /// before continuing. Waits for any scheduled batched_tick to complete
    /// and then runs additional ticks until all storage is flushed.
    pub async fn flush(&self) -> Result<(), RuntimeError> {
        let mut attempts = 0;
        loop {
            // Wait for any scheduled batched_tick to complete
            loop {
                let is_scheduled = {
                    let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
                    core.scheduler().is_scheduled()
                };

                if !is_scheduled {
                    break;
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;

                attempts += 1;
                if attempts > 200 {
                    break;
                }
            }

            // Synchronous tick and check if more work was generated
            let has_more_work = {
                let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
                core.batched_tick();
                core.has_outbound() || core.scheduler().is_scheduled()
            };

            if !has_more_work {
                break;
            }

            attempts += 1;
            if attempts > 200 {
                break;
            }
        }

        Ok(())
    }

    // =========================================================================
    // Queries
    // =========================================================================

    /// Execute a one-shot query with durability options.
    pub fn query(
        &self,
        query: Query,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
    ) -> Result<QueryFuture, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.query_with_propagation(query, session, durability, QueryPropagation::Full))
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
    pub fn subscribe<F>(
        &self,
        query: Query,
        callback: F,
        session: Option<Session>,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.subscribe(query, callback, session)
            .map_err(|e| RuntimeError::QueryError(e.to_string()))
    }

    /// Subscribe to a query with explicit durability and propagation options.
    pub fn subscribe_with_durability_and_propagation<F>(
        &self,
        query: Query,
        callback: F,
        session: Option<Session>,
        durability: ReadDurabilityOptions,
        propagation: QueryPropagation,
    ) -> Result<SubscriptionHandle, RuntimeError>
    where
        F: Fn(SubscriptionDelta) + Send + 'static,
    {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.subscribe_with_durability_and_propagation(
            query,
            callback,
            session,
            durability,
            propagation,
        )
        .map_err(|e| RuntimeError::QueryError(e.to_string()))
    }

    /// Unsubscribe from a query.
    pub fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.unsubscribe(handle);
        Ok(())
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    /// Push a sync message to the inbox (from network).
    pub fn push_sync_inbox(&self, entry: InboxEntry) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.park_sync_message(entry);
        Ok(())
    }

    /// Push a sync message with an explicit stream sequence (from network).
    pub fn push_sync_inbox_with_sequence(
        &self,
        entry: InboxEntry,
        sequence: u64,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.park_sync_message_with_sequence(entry, sequence);
        Ok(())
    }

    /// Set the next expected stream sequence for a server.
    pub fn set_server_next_sequence(
        &self,
        server_id: ServerId,
        next_sequence: u64,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.set_next_expected_server_sequence(server_id, next_sequence);
        Ok(())
    }

    /// Add a server connection.
    pub fn add_server(&self, server_id: ServerId) -> Result<(), RuntimeError> {
        self.add_server_with_catalogue_state_hash(server_id, None)
    }

    /// Add a server connection, optionally comparing the upstream catalogue
    /// digest first so unchanged catalogue objects are not replayed.
    pub fn add_server_with_catalogue_state_hash(
        &self,
        server_id: ServerId,
        remote_catalogue_state_hash: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.add_server_with_catalogue_state_hash(server_id, remote_catalogue_state_hash);
        Ok(())
    }

    /// Remove a server connection.
    pub fn remove_server(&self, server_id: ServerId) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.remove_server(server_id);
        Ok(())
    }

    /// Add a client connection.
    pub fn add_client(
        &self,
        client_id: ClientId,
        session: Option<Session>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.add_client(client_id, session);
        Ok(())
    }

    /// Ensure a client exists with the given session.
    ///
    /// A session is always required — callers must authenticate before
    /// registering a client.
    pub fn ensure_client_with_session(
        &self,
        client_id: ClientId,
        session: Session,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.ensure_client_with_session(client_id, session);
        Ok(())
    }

    /// Ensure a client exists and is marked as Backend without resetting state.
    pub fn ensure_client_as_backend(&self, client_id: ClientId) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.ensure_client_as_backend(client_id);
        Ok(())
    }

    /// Ensure a client exists and is marked as Admin without resetting state.
    pub fn ensure_client_as_admin(&self, client_id: ClientId) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.ensure_client_as_admin(client_id);
        Ok(())
    }

    /// Remove a client connection.
    ///
    /// Returns `Ok(true)` if removed, `Ok(false)` if skipped due to
    /// unprocessed inbox entries (caller should retry later).
    pub fn remove_client(&self, client_id: ClientId) -> Result<bool, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.remove_client(client_id))
    }

    /// Promote a client to Admin role (full access, no ReBAC).
    pub fn set_client_admin(&self, client_id: ClientId) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.set_client_admin(client_id);
        Ok(())
    }

    /// Promote a client to Backend role (row access, no catalogue writes).
    pub fn set_client_backend(&self, client_id: ClientId) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.set_client_backend(client_id);
        Ok(())
    }

    // =========================================================================
    // Schema Access
    // =========================================================================

    /// Get a clone of the current schema.
    pub fn current_schema(&self) -> Result<Schema, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.current_schema().clone())
    }

    /// Return all known schema hashes (for server mode).
    pub fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.schema_manager().known_schema_hashes())
    }

    /// Return a canonical digest of the runtime's catalogue state.
    pub fn catalogue_state_hash(&self) -> Result<String, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.schema_manager().catalogue_state_hash())
    }

    /// Get a known schema by hash from catalogue state.
    pub fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.schema_manager().get_known_schema(schema_hash).cloned())
    }

    /// Return the latest publish timestamp for a schema catalogue object.
    pub fn schema_published_at(
        &self,
        schema_hash: &SchemaHash,
    ) -> Result<Option<u64>, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.schema_manager().schema_published_at(schema_hash))
    }

    /// Seed an additional known schema into the in-memory schema manager.
    pub fn add_known_schema(&self, schema: Schema) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.schema_manager_mut().add_known_schema(schema);
        Ok(())
    }

    /// Return grouped telemetry for active downstream server subscriptions.
    pub fn server_subscription_telemetry(
        &self,
    ) -> Result<Vec<crate::query_manager::manager::ServerSubscriptionTelemetryGroup>, RuntimeError>
    {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core
            .schema_manager()
            .query_manager()
            .server_subscription_telemetry())
    }

    /// Access the underlying storage (for flushing, etc).
    ///
    /// The callback receives `&S` while holding the core lock.
    pub fn with_storage<R>(&self, f: impl FnOnce(&S) -> R) -> Result<R, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(f(core.storage()))
    }

    /// Load one replayable local batch record by logical batch id.
    pub fn local_batch_record(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<LocalBatchRecord>, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.local_batch_record(batch_id)?)
    }

    /// Scan all replayable local batch records retained by this runtime.
    pub fn local_batch_records(&self) -> Result<Vec<LocalBatchRecord>, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.local_batch_records()?)
    }

    /// Acknowledge a replayable rejected batch outcome and prune its local
    /// replay record.
    pub fn acknowledge_rejected_batch(&self, batch_id: BatchId) -> Result<bool, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.acknowledge_rejected_batch(batch_id)?)
    }

    /// Run a closure with read access to the SyncManager (for testing/inspection).
    #[cfg(test)]
    pub(crate) fn with_sync_manager<R>(
        &self,
        f: impl FnOnce(&crate::sync_manager::SyncManager) -> R,
    ) -> Result<R, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(f(core.schema_manager().query_manager().sync_manager()))
    }

    /// Access the underlying schema manager while holding the core lock.
    pub fn with_schema_manager<R>(
        &self,
        f: impl FnOnce(&SchemaManager) -> R,
    ) -> Result<R, RuntimeError> {
        let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(f(core.schema_manager()))
    }

    /// Subscribe to a query with explicit schema context (for server use).
    pub fn subscribe_with_schema_context(
        &self,
        query: Query,
        schema_context: &QuerySchemaContext,
        session: Option<Session>,
    ) -> Result<crate::sync_manager::QueryId, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        let result = core
            .subscribe_with_schema_context(query, schema_context, session)
            .map_err(|e| RuntimeError::QueryError(e.to_string()))?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
    use crate::schema_manager::AppId;
    use crate::storage::MemoryStorage;
    use crate::sync_manager::SyncManager;
    use std::sync::atomic::AtomicUsize;

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    fn user_row_values(id: ObjectId, name: &str) -> Vec<Value> {
        vec![Value::Uuid(id), Value::Text(name.to_string())]
    }

    fn user_insert_values(id: ObjectId, name: &str) -> HashMap<String, Value> {
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("name".to_string(), Value::Text(name.to_string())),
        ])
    }

    #[tokio::test]
    async fn test_runtime_insert_query() {
        let schema = test_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();

        let sync_count = Arc::new(AtomicUsize::new(0));
        let sync_count_clone = sync_count.clone();

        let runtime = TokioRuntime::new(schema_manager, MemoryStorage::new(), move |_msg| {
            sync_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Insert a row
        let user_id = ObjectId::new();
        let expected_values = user_row_values(user_id, "Alice");
        let (object_id, row_values) = runtime
            .insert("users", user_insert_values(user_id, "Alice"), None)
            .unwrap();
        assert!(!object_id.0.is_nil());
        assert_eq!(row_values, expected_values);

        // Query
        let query = Query::new("users");
        let future = runtime
            .query(query, None, ReadDurabilityOptions::default())
            .unwrap();
        let results = future.await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, object_id);
    }

    #[tokio::test]
    async fn test_runtime_update_delete() {
        let schema = test_schema();
        let app_id = AppId::from_name("test-crud");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();

        let runtime = TokioRuntime::new(schema_manager, MemoryStorage::new(), |_| {});

        // Insert
        let (object_id, _row_values) = runtime
            .insert("users", user_insert_values(ObjectId::new(), "Bob"), None)
            .unwrap();

        // Update
        let updates = vec![("name".to_string(), Value::Text("Charlie".to_string()))];
        runtime.update(object_id, updates, None).unwrap();

        // Verify update
        let query = Query::new("users");
        let future = runtime
            .query(query, None, ReadDurabilityOptions::default())
            .unwrap();
        let results = future.await.unwrap();
        assert_eq!(results[0].1[1], Value::Text("Charlie".to_string()));

        // Delete
        runtime.delete(object_id, None).unwrap();

        // Verify deleted
        let query = Query::new("users");
        let future = runtime
            .query(query, None, ReadDurabilityOptions::default())
            .unwrap();
        let results = future.await.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_subscription_callback() {
        use std::sync::Mutex;

        let schema = test_schema();
        let app_id = AppId::from_name("test-subscription");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();

        let runtime = TokioRuntime::new(schema_manager, MemoryStorage::new(), |_| {});

        // Track callback invocations
        let updates: Arc<Mutex<Vec<SubscriptionDelta>>> = Arc::new(Mutex::new(Vec::new()));
        let updates_clone = updates.clone();

        // Subscribe to users table
        let query = Query::new("users");
        let handle = runtime
            .subscribe(
                query,
                move |delta| {
                    updates_clone.lock().unwrap().push(delta);
                },
                None,
            )
            .unwrap();

        // Insert a row - this should trigger the subscription callback
        let (_object_id, _row_values) = runtime
            .insert("users", user_insert_values(ObjectId::new(), "Eve"), None)
            .unwrap();

        // Verify callback was invoked
        let updates_vec = updates.lock().unwrap();
        assert!(
            !updates_vec.is_empty(),
            "Subscription callback should have been invoked after insert"
        );
        assert_eq!(updates_vec[0].handle, handle);

        // Cleanup
        drop(updates_vec);
        runtime.unsubscribe(handle).unwrap();
    }
}

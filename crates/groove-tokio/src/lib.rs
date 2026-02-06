//! Tokio runtime adapter for Groove.
//!
//! Provides `TokioRuntime` - a thin wrapper around `RuntimeCore<TokioIoHandler>`
//! that handles async scheduling via `tokio::spawn`.
//!
//! # Architecture
//!
//! - `TokioIoHandler` implements `IoHandler` using tokio::spawn for scheduling
//! - `TokioRuntime` wraps `Arc<Mutex<RuntimeCore<TokioIoHandler>>>`
//! - Methods grab the lock, call RuntimeCore, and return
//! - `schedule_batched_tick` spawns a task that calls `batched_tick`
//! - No event loop - scheduling emerges from the IoHandler
//!
//! # Example
//!
//! ```ignore
//! use groove_tokio::TokioRuntime;
//! use groove::schema_manager::{SchemaManager, AppId};
//!
//! let schema_manager = SchemaManager::new(/* ... */);
//! let runtime = TokioRuntime::new(schema_manager, |msg| {
//!     // Handle sync messages
//! });
//!
//! // Direct method calls - no spawning needed
//! runtime.insert("users", values)?;
//! let future = runtime.query(query);
//! let results = future.await?;
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use groove::commit::{Commit, CommitId};
use groove::io_handler::{IoHandler, LoadedBranch, MemoryIoHandler};
use groove::object::{BranchName, ObjectId};
use groove::query_manager::query::Query;
use groove::query_manager::session::Session;
use groove::query_manager::types::{Schema, Value};
pub use groove::runtime_core::SubscriptionHandle;
use groove::runtime_core::{
    QueryFuture, RuntimeCore, RuntimeError as CoreRuntimeError, SubscriptionDelta,
};
use groove::schema_manager::{QuerySchemaContext, SchemaManager};
use groove::storage::{ContentHash, StorageError};
use groove::sync_manager::{
    ClientId, InboxEntry, OutboxEntry, PersistenceTier, QueryId, ServerId,
};

// ============================================================================
// TokioIoHandler
// ============================================================================

/// IoHandler implementation for Tokio.
///
/// Wraps `MemoryIoHandler` for all synchronous storage/index operations.
/// Adds a sync callback and tokio-based batched tick scheduling.
pub struct TokioIoHandler {
    /// Delegate all storage/index ops to MemoryIoHandler.
    inner: MemoryIoHandler,
    /// Callback for sync messages.
    sync_callback: Arc<dyn Fn(OutboxEntry) + Send + Sync>,
    /// Debounce flag for scheduled ticks.
    scheduled: Arc<AtomicBool>,
    /// Weak reference back to RuntimeCore for spawned tasks.
    core_ref: Weak<Mutex<RuntimeCore<TokioIoHandler>>>,
}

impl TokioIoHandler {
    /// Create a new TokioIoHandler.
    ///
    /// Note: `core_ref` starts as empty and is set after RuntimeCore is created.
    fn new<F>(sync_callback: F) -> Self
    where
        F: Fn(OutboxEntry) + Send + Sync + 'static,
    {
        Self {
            inner: MemoryIoHandler::new(),
            sync_callback: Arc::new(sync_callback),
            scheduled: Arc::new(AtomicBool::new(false)),
            core_ref: Weak::new(),
        }
    }

    /// Set the core reference (called after RuntimeCore is wrapped in Arc<Mutex>).
    fn set_core_ref(&mut self, core_ref: Weak<Mutex<RuntimeCore<TokioIoHandler>>>) {
        self.core_ref = core_ref;
    }

    /// Check if a batched_tick is currently scheduled.
    pub fn is_scheduled(&self) -> bool {
        self.scheduled.load(Ordering::SeqCst)
    }
}

impl IoHandler for TokioIoHandler {
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.inner.create_object(id, metadata)
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.inner.load_object_metadata(id)
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.inner.load_branch(object_id, branch)
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        self.inner.append_commit(object_id, branch, commit)
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        self.inner.delete_commit(object_id, branch, commit_id)
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        self.inner.set_branch_tails(object_id, branch, tails)
    }

    fn store_blob(&mut self, hash: ContentHash, data: &[u8]) -> Result<(), StorageError> {
        self.inner.store_blob(hash, data)
    }

    fn load_blob(&self, hash: ContentHash) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.load_blob(hash)
    }

    fn delete_blob(&mut self, hash: ContentHash) -> Result<(), StorageError> {
        self.inner.delete_blob(hash)
    }

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: PersistenceTier,
    ) -> Result<(), StorageError> {
        self.inner.store_ack_tier(commit_id, tier)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.index_remove(table, column, branch, value, row_id)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.inner.index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.inner.index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.inner.index_scan_all(table, column, branch)
    }

    fn send_sync_message(&mut self, message: OutboxEntry) {
        (self.sync_callback)(message);
    }

    fn schedule_batched_tick(&self) {
        // Debounce: only schedule if not already scheduled
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            let core_ref = self.core_ref.clone();
            let flag = self.scheduled.clone();

            tokio::spawn(async move {
                // Call batched_tick on the core
                if let Some(core_arc) = core_ref.upgrade() {
                    if let Ok(mut core) = core_arc.lock() {
                        core.batched_tick();
                    }
                }

                // Clear the scheduled flag AFTER tick completes
                flag.store(false, Ordering::SeqCst);
            });
        }
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

/// Tokio runtime for Groove.
///
/// Thin wrapper around `Arc<Mutex<RuntimeCore<TokioIoHandler>>>`.
/// All methods grab the lock, call RuntimeCore, and return.
/// Async scheduling happens via IoHandler.schedule_batched_tick().
#[derive(Clone)]
pub struct TokioRuntime {
    core: Arc<Mutex<RuntimeCore<TokioIoHandler>>>,
}

impl TokioRuntime {
    /// Create a new TokioRuntime.
    ///
    /// # Arguments
    /// - `schema_manager` - The SchemaManager to wrap
    /// - `sync_callback` - Called when sync messages need to be sent
    pub fn new<F>(schema_manager: SchemaManager, sync_callback: F) -> Self
    where
        F: Fn(OutboxEntry) + Send + Sync + 'static,
    {
        // Create IoHandler (without core_ref initially)
        let io_handler = TokioIoHandler::new(sync_callback);

        // Create RuntimeCore
        let core = RuntimeCore::new(schema_manager, io_handler);

        // Wrap in Arc<Mutex>
        let core_arc = Arc::new(Mutex::new(core));

        // Set the core_ref on the IoHandler
        {
            let mut core_guard = core_arc.lock().unwrap();
            core_guard
                .io_handler_mut()
                .set_core_ref(Arc::downgrade(&core_arc));
        }

        Self { core: core_arc }
    }

    /// Persist the current schema to the catalogue for server sync.
    pub fn persist_schema(&self) -> Result<ObjectId, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.persist_schema())
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    /// Insert a row into a table.
    ///
    /// This is fire-and-forget - the insert is queued and persistence happens
    /// via IoHandler scheduling. Callers who need synchronous persistence
    /// should call `flush()` after this method.
    pub fn insert(
        &self,
        table: &str,
        values: Vec<Value>,
        session: Option<&Session>,
    ) -> Result<ObjectId, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        let result = core.insert(table, values, session)?;
        // immediate_tick is called by RuntimeCore::insert
        Ok(result)
    }

    /// Update a row (partial update by column name).
    ///
    /// This is fire-and-forget - the update is queued and persistence happens
    /// via IoHandler scheduling. Callers who need synchronous persistence
    /// should call `flush()` after this method.
    pub fn update(
        &self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.update(object_id, values, session)?;
        // immediate_tick is called by RuntimeCore::update
        Ok(())
    }

    /// Delete a row.
    ///
    /// This is fire-and-forget - the delete is queued and persistence happens
    /// via IoHandler scheduling. Callers who need synchronous persistence
    /// should call `flush()` after this method.
    pub fn delete(
        &self,
        object_id: ObjectId,
        session: Option<&Session>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.delete(object_id, session)?;
        // immediate_tick is called by RuntimeCore::delete
        Ok(())
    }

    /// Flush pending operations to storage.
    ///
    /// Call this after CRUD operations if you need to ensure data is persisted
    /// before continuing. This waits for any scheduled batched_tick to complete
    /// and then runs additional ticks until all storage is flushed.
    ///
    /// For most use cases, relying on IoHandler scheduling via
    /// `schedule_batched_tick()` is sufficient.
    pub async fn flush(&self) -> Result<(), RuntimeError> {
        // Keep flushing until everything is stable:
        // - No scheduled tasks pending
        // - No outbound messages after our final tick
        let mut attempts = 0;
        loop {
            // First, wait for any scheduled batched_tick to complete
            loop {
                let is_scheduled = {
                    let core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
                    core.io_handler().is_scheduled()
                };

                if !is_scheduled {
                    break;
                }

                // Sleep briefly to allow the scheduled task to run
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;

                attempts += 1;
                if attempts > 200 {
                    // Safety valve
                    break;
                }
            }

            // Now do a synchronous tick and check if more work was generated
            let has_more_work = {
                let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
                core.batched_tick();
                // Check if processing created more outbound work
                core.has_outbound() || core.io_handler().is_scheduled()
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

    /// Execute a one-shot query.
    ///
    /// Returns a future that resolves when the query completes.
    pub fn query(
        &self,
        query: Query,
        session: Option<Session>,
    ) -> Result<QueryFuture, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(core.query(query, session))
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
    ///
    /// The callback is invoked when results change.
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
            .map_err(|e| RuntimeError::QueryError(format!("{:?}", e)))
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

    /// Add a server connection.
    pub fn add_server(&self, server_id: ServerId) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.add_server(server_id);
        // immediate_tick is called by RuntimeCore::add_server
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
        // immediate_tick is called by RuntimeCore::add_client
        Ok(())
    }

    /// Ensure a client exists with the given session.
    ///
    /// If the client already exists with the same session, this is a no-op.
    /// If the client exists with a different session, we currently panic with todo!()
    /// as session migration is not yet implemented.
    /// If the client doesn't exist, it's added with the given session.
    pub fn ensure_client_with_session(
        &self,
        client_id: ClientId,
        session: Option<Session>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.ensure_client_with_session(client_id, session);
        Ok(())
    }

    /// Add a client connection and sync all data to them.
    pub fn add_client_with_full_sync(
        &self,
        client_id: ClientId,
        session: Option<Session>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.add_client_with_full_sync(client_id, session);
        // immediate_tick is called by RuntimeCore::add_client_with_full_sync
        Ok(())
    }

    /// Remove a client connection.
    pub fn remove_client(&self, client_id: ClientId) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.remove_client(client_id);
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

    /// Subscribe to a query with explicit schema context (for server use).
    ///
    /// This is used by servers to create subscriptions on behalf of clients
    /// that may be using different schema versions.
    pub fn subscribe_with_schema_context(
        &self,
        query: Query,
        schema_context: &QuerySchemaContext,
        session: Option<Session>,
    ) -> Result<QueryId, RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        let result = core
            .subscribe_with_schema_context(query, schema_context, session)
            .map_err(|e| RuntimeError::QueryError(format!("{:?}", e)))?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
    use groove::schema_manager::AppId;
    use groove::sync_manager::SyncManager;
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

    #[tokio::test]
    async fn test_runtime_insert_query() {
        let schema = test_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();

        let sync_count = Arc::new(AtomicUsize::new(0));
        let sync_count_clone = sync_count.clone();

        let runtime = TokioRuntime::new(schema_manager, move |_msg| {
            sync_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Insert a row
        let values = vec![
            Value::Uuid(ObjectId::new()),
            Value::Text("Alice".to_string()),
        ];
        let object_id = runtime.insert("users", values, None).unwrap();
        assert!(!object_id.0.is_nil());

        // Query
        let query = Query::new("users");
        let future = runtime.query(query, None).unwrap();
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

        let runtime = TokioRuntime::new(schema_manager, |_| {});

        // Insert
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Bob".to_string())];
        let object_id = runtime.insert("users", values, None).unwrap();

        // Update
        let updates = vec![("name".to_string(), Value::Text("Charlie".to_string()))];
        runtime.update(object_id, updates, None).unwrap();

        // Verify update
        let query = Query::new("users");
        let future = runtime.query(query, None).unwrap();
        let results = future.await.unwrap();
        assert_eq!(results[0].1[1], Value::Text("Charlie".to_string()));

        // Delete
        runtime.delete(object_id, None).unwrap();

        // Verify deleted
        let query = Query::new("users");
        let future = runtime.query(query, None).unwrap();
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

        let runtime = TokioRuntime::new(schema_manager, |_| {});

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
        let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Eve".to_string())];
        let _object_id = runtime.insert("users", values, None).unwrap();

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

//! Async runtime adapter for Groove.
//!
//! Wraps the synchronous groove managers for tokio integration,
//! providing an event loop that drives storage operations.
//!
//! # Example
//!
//! ```ignore
//! use groove_runtime::JazzRuntime;
//! use groove_rocksdb::RocksDbDriver;
//! use groove::schema_manager::{SchemaManager, AppId};
//!
//! let driver = RocksDbDriver::open("./data").unwrap();
//! let schema_manager = SchemaManager::new(/* ... */);
//! let runtime = JazzRuntime::new(schema_manager, driver);
//!
//! // Run the event loop
//! tokio::spawn(async move {
//!     runtime.run().await;
//! });
//! ```

use groove::driver::Driver;
use groove::object::ObjectId;
use groove::query_manager::QuerySubscriptionId;
use groove::query_manager::manager::QueryError;
use groove::query_manager::query::Query;
use groove::query_manager::session::Session;
use groove::query_manager::types::{RowDelta, Schema, TableName, Value};
use groove::schema_manager::{QuerySchemaContext, SchemaManager};
use groove::sync_manager::{ClientId, InboxEntry, OutboxEntry, QueryId, ServerId};

use tokio::sync::{mpsc, oneshot};

/// Commands that can be sent to the runtime.
#[derive(Debug)]
pub enum RuntimeCommand {
    /// Subscribe to a query.
    Subscribe {
        query: Query,
        session: Option<Session>,
        respond: oneshot::Sender<SubscriptionHandle>,
    },

    /// Unsubscribe from a query.
    Unsubscribe { handle: SubscriptionHandle },

    /// Execute a one-shot query.
    Query {
        query: Query,
        session: Option<Session>,
        respond: oneshot::Sender<Result<Vec<(ObjectId, Vec<Value>)>, RuntimeError>>,
    },

    /// Insert a row into a table.
    Insert {
        table: String,
        values: Vec<Value>,
        session: Option<Session>,
        respond: oneshot::Sender<Result<ObjectId, RuntimeError>>,
    },

    /// Update a row.
    Update {
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<Session>,
        respond: oneshot::Sender<Result<(), RuntimeError>>,
    },

    /// Delete a row.
    Delete {
        object_id: ObjectId,
        session: Option<Session>,
        respond: oneshot::Sender<Result<(), RuntimeError>>,
    },

    /// Push sync message to inbox (from network).
    SyncInbox { entry: InboxEntry },

    /// Add a server connection.
    AddServer { server_id: ServerId },

    /// Remove a server connection.
    RemoveServer { server_id: ServerId },

    /// Add a client connection.
    AddClient {
        client_id: ClientId,
        session: Option<Session>,
    },

    /// Add a client connection and sync all data to them.
    AddClientWithFullSync {
        client_id: ClientId,
        session: Option<Session>,
    },

    /// Remove a client connection.
    RemoveClient { client_id: ClientId },

    /// Get the current schema.
    GetSchema { respond: oneshot::Sender<Schema> },

    /// Subscribe with explicit schema context (for server use).
    ///
    /// Servers use this to subscribe using the client's schema context
    /// rather than the server's own schema context.
    SubscribeWithSchemaContext {
        query: Query,
        schema_context: QuerySchemaContext,
        session: Option<Session>,
        respond: oneshot::Sender<Result<QueryId, QueryError>>,
    },

    /// Shutdown the runtime.
    Shutdown,
}

/// Handle to a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub u64);

/// Errors from runtime operations.
#[derive(Debug, Clone)]
pub enum RuntimeError {
    QueryError(String),
    WriteError(String),
    NotFound,
    ChannelClosed,
}

/// A query waiting for sync/storage to complete.
struct PendingQuery {
    query: Query,
    #[allow(dead_code)]
    session: Option<Session>,
    respond: oneshot::Sender<Result<Vec<(ObjectId, Vec<Value>)>, RuntimeError>>,
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::QueryError(s) => write!(f, "Query error: {}", s),
            RuntimeError::WriteError(s) => write!(f, "Write error: {}", s),
            RuntimeError::NotFound => write!(f, "Not found"),
            RuntimeError::ChannelClosed => write!(f, "Channel closed"),
        }
    }
}

impl std::error::Error for RuntimeError {}

/// Event emitted by the runtime.
#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    /// Subscription results changed.
    SubscriptionUpdate {
        handle: SubscriptionHandle,
        delta: RowDelta,
    },

    /// Sync message to send (from outbox).
    SyncOutbox(OutboxEntry),
}

/// Handle for interacting with a running JazzRuntime.
#[derive(Clone)]
pub struct RuntimeHandle {
    commands: mpsc::Sender<RuntimeCommand>,
}

impl RuntimeHandle {
    /// Subscribe to a query.
    pub async fn subscribe(
        &self,
        query: Query,
        session: Option<Session>,
    ) -> Result<SubscriptionHandle, RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Subscribe {
                query,
                session,
                respond: tx,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)?;
        rx.await.map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Unsubscribe from a query.
    pub async fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<(), RuntimeError> {
        self.commands
            .send(RuntimeCommand::Unsubscribe { handle })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Execute a one-shot query.
    pub async fn query(
        &self,
        query: Query,
        session: Option<Session>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>, RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Query {
                query,
                session,
                respond: tx,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)?;
        rx.await.map_err(|_| RuntimeError::ChannelClosed)?
    }

    /// Insert a row into a table.
    pub async fn insert(
        &self,
        table: &str,
        values: Vec<Value>,
        session: Option<Session>,
    ) -> Result<ObjectId, RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Insert {
                table: table.to_string(),
                values,
                session,
                respond: tx,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)?;
        rx.await.map_err(|_| RuntimeError::ChannelClosed)?
    }

    /// Update a row.
    pub async fn update(
        &self,
        object_id: ObjectId,
        values: Vec<(String, Value)>,
        session: Option<Session>,
    ) -> Result<(), RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Update {
                object_id,
                values,
                session,
                respond: tx,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)?;
        rx.await.map_err(|_| RuntimeError::ChannelClosed)?
    }

    /// Delete a row.
    pub async fn delete(
        &self,
        object_id: ObjectId,
        session: Option<Session>,
    ) -> Result<(), RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Delete {
                object_id,
                session,
                respond: tx,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)?;
        rx.await.map_err(|_| RuntimeError::ChannelClosed)?
    }

    /// Push a sync message to the inbox.
    pub async fn push_sync_inbox(&self, entry: InboxEntry) -> Result<(), RuntimeError> {
        self.commands
            .send(RuntimeCommand::SyncInbox { entry })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Add a server connection.
    pub async fn add_server(&self, server_id: ServerId) -> Result<(), RuntimeError> {
        self.commands
            .send(RuntimeCommand::AddServer { server_id })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Remove a server connection.
    pub async fn remove_server(&self, server_id: ServerId) -> Result<(), RuntimeError> {
        self.commands
            .send(RuntimeCommand::RemoveServer { server_id })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Add a client connection.
    pub async fn add_client(
        &self,
        client_id: ClientId,
        session: Option<Session>,
    ) -> Result<(), RuntimeError> {
        self.commands
            .send(RuntimeCommand::AddClient { client_id, session })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Add a client connection and sync all data to them.
    ///
    /// Use this when new clients should receive all server data immediately.
    pub async fn add_client_with_full_sync(
        &self,
        client_id: ClientId,
        session: Option<Session>,
    ) -> Result<(), RuntimeError> {
        self.commands
            .send(RuntimeCommand::AddClientWithFullSync { client_id, session })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Remove a client connection.
    pub async fn remove_client(&self, client_id: ClientId) -> Result<(), RuntimeError> {
        self.commands
            .send(RuntimeCommand::RemoveClient { client_id })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Get the current schema.
    pub async fn get_schema(&self) -> Result<Schema, RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::GetSchema { respond: tx })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)?;
        rx.await.map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Shutdown the runtime.
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        self.commands
            .send(RuntimeCommand::Shutdown)
            .await
            .map_err(|_| RuntimeError::ChannelClosed)
    }

    /// Subscribe to a query with explicit schema context (for server use).
    ///
    /// Servers use this to execute queries using the client's schema context
    /// rather than the server's own schema.
    pub async fn subscribe_with_schema_context(
        &self,
        query: Query,
        schema_context: &QuerySchemaContext,
        session: Option<Session>,
    ) -> Result<QueryId, RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::SubscribeWithSchemaContext {
                query,
                schema_context: schema_context.clone(),
                session,
                respond: tx,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed)?;
        rx.await
            .map_err(|_| RuntimeError::ChannelClosed)?
            .map_err(|e| RuntimeError::QueryError(format!("{:?}", e)))
    }
}

/// Async runtime that drives all manager layers.
pub struct JazzRuntime<D: Driver + Send> {
    schema_manager: SchemaManager,
    driver: D,
    commands: mpsc::Receiver<RuntimeCommand>,
    events: mpsc::Sender<RuntimeEvent>,
    next_subscription_handle: u64,
    /// Map from our handles to QueryManager's internal subscription IDs
    subscriptions: std::collections::HashMap<SubscriptionHandle, QuerySubscriptionId>,
    /// Reverse map for routing updates back to handles
    subscription_reverse: std::collections::HashMap<QuerySubscriptionId, SubscriptionHandle>,
    /// Queries waiting for sync to deliver objects
    pending_queries: Vec<PendingQuery>,
}

impl<D: Driver + Send + 'static> JazzRuntime<D> {
    /// Create a new runtime with a SchemaManager and Driver.
    ///
    /// Returns the runtime and a handle for sending commands.
    pub fn new(
        mut schema_manager: SchemaManager,
        mut driver: D,
    ) -> (Self, RuntimeHandle, mpsc::Receiver<RuntimeEvent>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let (evt_tx, evt_rx) = mpsc::channel(256);

        // Load existing data from storage (cold start)
        schema_manager
            .query_manager_mut()
            .load_indices_from_driver(&mut driver);

        let runtime = Self {
            schema_manager,
            driver,
            commands: cmd_rx,
            events: evt_tx,
            next_subscription_handle: 0,
            subscriptions: std::collections::HashMap::new(),
            subscription_reverse: std::collections::HashMap::new(),
            pending_queries: Vec::new(),
        };

        let handle = RuntimeHandle { commands: cmd_tx };

        (runtime, handle, evt_rx)
    }

    /// Process storage requests, returning true if work was done.
    fn tick_storage(&mut self) -> bool {
        self.schema_manager
            .query_manager_mut()
            .process_storage_with_driver(&mut self.driver)
    }

    /// Execute a query, processing storage in a tight loop until blocked on sync.
    ///
    /// Returns Ok(results) if query completes, or Err(Pending) if blocked on sync.
    fn execute_query_with_storage(
        &mut self,
        query: &Query,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>, groove::query_manager::QueryError> {
        loop {
            match self.schema_manager.execute(query.clone()) {
                Ok(results) => return Ok(results),
                Err(groove::query_manager::QueryError::Pending) => {
                    // Try to make progress with storage
                    let made_progress = self.tick_storage();
                    if !made_progress {
                        // No storage work - truly blocked on sync
                        return Err(groove::query_manager::QueryError::Pending);
                    }
                    // Made progress, retry immediately
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Retry pending queries - called during tick() after sync inbox is processed.
    fn retry_pending_queries(&mut self) {
        // Drain pending_queries, retry each, re-add if still pending
        let queries = std::mem::take(&mut self.pending_queries);

        for pending in queries {
            // Try to complete with storage loop
            let result = self.execute_query_with_storage(&pending.query);

            match result {
                Ok(results) => {
                    // Query complete - send response
                    let _ = pending.respond.send(Ok(results));
                }
                Err(groove::query_manager::QueryError::Pending) => {
                    // Still pending - put back in queue
                    self.pending_queries.push(pending);
                }
                Err(e) => {
                    // Real error - send error response
                    let _ = pending
                        .respond
                        .send(Err(RuntimeError::QueryError(format!("{:?}", e))));
                }
            }
        }
    }

    /// Run the event loop until shutdown.
    pub async fn run(mut self) {
        use tokio::time::{Duration, interval};

        let mut tick_interval = interval(Duration::from_millis(10));

        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    self.tick();
                }
                Some(cmd) = self.commands.recv() => {
                    if self.handle_command(cmd) {
                        // Final tick to flush any pending storage
                        self.tick();
                        break; // Shutdown requested
                    }
                }
            }
        }
    }

    /// Process one tick: flush storage, process inbox, emit events.
    pub fn tick(&mut self) {
        // 1. Process storage requests through driver (runtime-specific)
        self.schema_manager
            .query_manager_mut()
            .process_storage_with_driver(&mut self.driver);

        // 2. Use shared tick logic: process + collect outbox + collect updates
        let result = self.schema_manager.tick_settled();

        // 3. Emit sync outbox entries via channel (runtime-specific)
        for entry in result.outbox {
            let _ = self.events.try_send(RuntimeEvent::SyncOutbox(entry));
        }

        // 4. Emit subscription updates via channel (runtime-specific)
        for update in result.subscription_updates {
            if let Some(&handle) = self.subscription_reverse.get(&update.subscription_id) {
                let _ = self.events.try_send(RuntimeEvent::SubscriptionUpdate {
                    handle,
                    delta: update.delta,
                });
            }
        }

        // 5. Retry pending queries (runtime-specific, may complete after sync/storage)
        self.retry_pending_queries();
    }

    /// Handle a command, returning true if shutdown was requested.
    fn handle_command(&mut self, cmd: RuntimeCommand) -> bool {
        match cmd {
            RuntimeCommand::Subscribe {
                query,
                session,
                respond,
            } => {
                // Subscribe through QueryManager
                let result = self
                    .schema_manager
                    .query_manager_mut()
                    .subscribe_with_session(query, session);

                match result {
                    Ok(query_sub_id) => {
                        let handle = SubscriptionHandle(self.next_subscription_handle);
                        self.next_subscription_handle += 1;
                        self.subscriptions.insert(handle, query_sub_id);
                        self.subscription_reverse.insert(query_sub_id, handle);
                        let _ = respond.send(handle);
                    }
                    Err(_e) => {
                        // On error, still send a handle but don't track it
                        // (subscription won't receive updates)
                        let handle = SubscriptionHandle(self.next_subscription_handle);
                        self.next_subscription_handle += 1;
                        let _ = respond.send(handle);
                    }
                }
            }

            RuntimeCommand::Unsubscribe { handle } => {
                if let Some(query_sub_id) = self.subscriptions.remove(&handle) {
                    self.subscription_reverse.remove(&query_sub_id);
                    self.schema_manager
                        .query_manager_mut()
                        .unsubscribe(query_sub_id);
                }
            }

            RuntimeCommand::Query {
                query,
                session,
                respond,
            } => {
                // Execute query, exhausting storage work first
                let result = self.execute_query_with_storage(&query);

                match result {
                    Ok(results) => {
                        let _ = respond.send(Ok(results));
                    }
                    Err(groove::query_manager::QueryError::Pending) => {
                        // Blocked on sync - store for later completion
                        self.pending_queries.push(PendingQuery {
                            query,
                            session,
                            respond,
                        });
                    }
                    Err(e) => {
                        let _ = respond.send(Err(RuntimeError::QueryError(format!("{:?}", e))));
                    }
                }
            }

            RuntimeCommand::Insert {
                table,
                values,
                session,
                respond,
            } => {
                let result = if let Some(s) = session.as_ref() {
                    self.schema_manager
                        .insert_with_session(&table, &values, Some(s))
                } else {
                    self.schema_manager.insert(&table, &values)
                };
                match result {
                    Ok(insert_handle) => {
                        let _ = respond.send(Ok(insert_handle.row_id));
                    }
                    Err(e) => {
                        let _ = respond.send(Err(RuntimeError::WriteError(format!("{:?}", e))));
                    }
                }
            }

            RuntimeCommand::Update {
                object_id,
                values: partial_values,
                session,
                respond,
            } => {
                // Partial update: merge provided values with current row
                let result = (|| {
                    // Get current row and table name
                    let (table, mut current_values) = self
                        .schema_manager
                        .query_manager_mut()
                        .get_row(object_id)
                        .ok_or_else(|| RuntimeError::NotFound)?;

                    // Get schema to find column indices
                    let schema = self.schema_manager.current_schema();
                    let table_name = TableName::new(&table);
                    let table_schema = schema
                        .get(&table_name)
                        .ok_or_else(|| RuntimeError::WriteError("Table not found".to_string()))?;

                    // Merge partial updates
                    for (col_name, new_value) in partial_values {
                        if let Some(idx) = table_schema.descriptor.column_index(&col_name) {
                            current_values[idx] = new_value;
                        } else {
                            return Err(RuntimeError::WriteError(format!(
                                "Column '{}' not found",
                                col_name
                            )));
                        }
                    }

                    // Perform full update
                    self.schema_manager
                        .query_manager_mut()
                        .update_with_session(object_id, &current_values, session.as_ref())
                        .map_err(|e| RuntimeError::WriteError(format!("{:?}", e)))
                })();
                let _ = respond.send(result);
            }

            RuntimeCommand::Delete {
                object_id,
                session,
                respond,
            } => {
                let result = self
                    .schema_manager
                    .query_manager_mut()
                    .delete_with_session(object_id, session.as_ref());
                match result {
                    Ok(_delete_handle) => {
                        let _ = respond.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = respond.send(Err(RuntimeError::WriteError(format!("{:?}", e))));
                    }
                }
            }

            RuntimeCommand::SyncInbox { entry } => {
                self.schema_manager
                    .query_manager_mut()
                    .sync_manager_mut()
                    .push_inbox(entry);
            }

            RuntimeCommand::AddServer { server_id } => {
                self.schema_manager
                    .query_manager_mut()
                    .sync_manager_mut()
                    .add_server(server_id);
            }

            RuntimeCommand::RemoveServer { server_id } => {
                self.schema_manager
                    .query_manager_mut()
                    .sync_manager_mut()
                    .remove_server(server_id);
            }

            RuntimeCommand::AddClient { client_id, session } => {
                let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
                sm.add_client(client_id);
                if let Some(s) = session {
                    sm.set_client_session(client_id, s);
                }
            }

            RuntimeCommand::AddClientWithFullSync { client_id, session } => {
                let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
                sm.add_client_with_full_sync(client_id);
                if let Some(s) = session {
                    sm.set_client_session(client_id, s);
                }
            }

            RuntimeCommand::RemoveClient { client_id } => {
                self.schema_manager
                    .query_manager_mut()
                    .sync_manager_mut()
                    .remove_client(client_id);
            }

            RuntimeCommand::GetSchema { respond } => {
                let _ = respond.send(self.schema_manager.current_schema().clone());
            }

            RuntimeCommand::SubscribeWithSchemaContext {
                query,
                schema_context,
                session,
                respond,
            } => {
                let result = self.schema_manager.subscribe_with_schema_context(
                    query,
                    &schema_context,
                    session,
                );
                // Convert QuerySubscriptionId to QueryId for the sync protocol
                let response = result.map(|sub_id| QueryId(sub_id.0));
                let _ = respond.send(response);
            }

            RuntimeCommand::Shutdown => {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
    use groove::schema_manager::AppId;
    use groove::sync_manager::SyncManager;

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
    async fn test_runtime_creates_and_shuts_down() {
        let schema = test_schema();
        let app_id = AppId::from_name("test-app");
        let sync_manager = SyncManager::new();

        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();

        // Use TestDriver for this test
        let driver = groove::driver::TestDriver::new();

        let (runtime, handle, _events) = JazzRuntime::new(schema_manager, driver);

        // Spawn runtime
        let runtime_task = tokio::spawn(async move {
            runtime.run().await;
        });

        // Get schema
        let schema = handle.get_schema().await.unwrap();
        assert!(schema.contains_key(&groove::query_manager::types::TableName::new("users")));

        // Shutdown
        handle.shutdown().await.unwrap();

        // Wait for runtime to finish
        runtime_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_update_delete() {
        let schema = test_schema();
        let app_id = AppId::from_name("test-crud");
        let sync_manager = SyncManager::new();

        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();

        let driver = groove::driver::TestDriver::new();
        let (runtime, handle, _events) = JazzRuntime::new(schema_manager, driver);

        // Spawn runtime
        let runtime_task = tokio::spawn(async move {
            runtime.run().await;
        });

        // Insert a row
        let values = vec![
            Value::Uuid(ObjectId::new()),
            Value::Text("Alice".to_string()),
        ];
        let object_id = handle.insert("users", values.clone(), None).await.unwrap();
        assert!(!object_id.0.is_nil(), "Insert should return valid ObjectId");

        // Partial update
        let updates = vec![("name".to_string(), Value::Text("Bob".to_string()))];
        handle.update(object_id, updates, None).await.unwrap();

        // Delete
        handle.delete(object_id, None).await.unwrap();

        // Shutdown
        handle.shutdown().await.unwrap();
        runtime_task.await.unwrap();
    }
}

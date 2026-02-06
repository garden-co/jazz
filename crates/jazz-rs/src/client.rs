//! JazzClient implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use groove::query_manager::query::Query;
use groove::query_manager::session::Session;
use groove::query_manager::types::{RowDelta, Value};
use groove::schema_manager::SchemaManager;
use groove::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncManager};
use groove_tokio::{SubscriptionHandle as RuntimeSubHandle, TokioRuntime};
use jazz_transport::ServerEvent;
use reqwest_eventsource::{Event, EventSource};
use tokio::sync::{RwLock, mpsc};

use crate::transport::{AuthConfig, ServerConnection};
use crate::{AppContext, JazzError, ObjectId, Result, SubscriptionHandle, SubscriptionStream};

/// Jazz client for building applications.
///
/// Combines local persistence with server sync.
pub struct JazzClient {
    /// Handle to the local runtime.
    runtime: TokioRuntime,
    /// Connection to the server (shared for event processor).
    server_connection: Option<Arc<ServerConnection>>,
    /// Client configuration.
    #[allow(dead_code)]
    context: AppContext,
    /// Active subscriptions (metadata).
    subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, SubscriptionState>>>,
    /// Subscription delta senders (for routing deltas from callbacks to streams).
    subscription_senders: Arc<RwLock<HashMap<RuntimeSubHandle, mpsc::Sender<RowDelta>>>>,
    /// Next subscription handle ID.
    next_handle: std::sync::atomic::AtomicU64,
    /// Handle for the SSE listener task.
    sse_listener_task: Option<tokio::task::JoinHandle<()>>,
}

/// State for an active subscription.
#[allow(dead_code)]
struct SubscriptionState {
    query: Query,
    runtime_handle: RuntimeSubHandle,
    server_query_id: Option<groove::sync_manager::QueryId>,
}

impl JazzClient {
    /// Connect to Jazz with the given configuration.
    ///
    /// This will:
    /// 1. Open local RocksDB storage
    /// 2. Initialize the runtime
    /// 3. Connect to the server (if URL provided)
    /// 4. Start syncing
    pub async fn connect(context: AppContext) -> Result<Self> {
        // Create data directory if needed
        std::fs::create_dir_all(&context.data_dir)?;

        // Load or generate persistent client_id
        let client_id_path = context.data_dir.join("client_id");
        let client_id = if client_id_path.exists() {
            let id_str = std::fs::read_to_string(&client_id_path)?;
            ClientId::parse(id_str.trim()).unwrap_or_else(|| {
                // File corrupted - generate new ID and overwrite
                let id = context.client_id.unwrap_or_else(ClientId::new);
                let _ = std::fs::write(&client_id_path, id.to_string());
                id
            })
        } else if let Some(id) = context.client_id {
            // Use explicitly provided client_id and persist it
            std::fs::write(&client_id_path, id.to_string())?;
            id
        } else {
            // Generate new client_id and persist it
            let id = ClientId::new();
            std::fs::write(&client_id_path, id.to_string())?;
            id
        };

        // Create managers
        let sync_manager = SyncManager::new();
        let schema_manager = SchemaManager::new(
            sync_manager,
            context.schema.clone(),
            context.app_id,
            "client",
            "main",
        )
        .map_err(|e| JazzError::Schema(format!("{:?}", e)))?;

        // Connect to server if URL provided (before creating runtime so we have the connection)
        let auth_config = AuthConfig::from_context(&context);
        let server_connection = if !context.server_url.is_empty() {
            match ServerConnection::connect(&context.server_url, auth_config).await {
                Ok(conn) => Some(Arc::new(conn)),
                Err(e) => {
                    tracing::warn!("Failed to connect to server: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Clone server connection for sync callback
        let server_conn_for_sync = server_connection.clone();
        let client_id_for_sync = client_id;

        // Create runtime with sync callback
        let runtime = TokioRuntime::new(schema_manager, move |entry| {
            // Send to server if connected and destination is server
            if let Destination::Server(_) = entry.destination {
                eprintln!(
                    "DEBUG [client sync_cb]: Sending to server: {:?}",
                    entry.payload.variant_name()
                );
                if let Some(ref conn) = server_conn_for_sync {
                    let conn = conn.clone();
                    let payload = entry.payload.clone();
                    let cid = client_id_for_sync;
                    tokio::spawn(async move {
                        if let Err(e) = conn.push_sync(payload, cid).await {
                            tracing::warn!("Failed to push sync to server: {}", e);
                        }
                    });
                } else {
                    eprintln!("DEBUG [client sync_cb]: No server connection!");
                }
            }
        });

        // Persist schema to catalogue for server sync
        runtime
            .persist_schema()
            .map_err(|e| JazzError::Storage(e.to_string()))?;

        // Register server with sync manager if connected
        if server_connection.is_some() {
            let server_id = ServerId::default();
            if let Err(e) = runtime.add_server(server_id) {
                tracing::warn!("Failed to register server with sync manager: {}", e);
            }
        }

        // Create shared subscription senders map
        let subscription_senders: Arc<RwLock<HashMap<RuntimeSubHandle, mpsc::Sender<RowDelta>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Spawn SSE listener if connected to server
        let sse_listener_task = if let Some(ref conn) = server_connection {
            let base_url = conn.base_url().to_string();
            let client_id_str = client_id.to_string();
            let runtime_for_sse = runtime.clone();

            Some(tokio::spawn(async move {
                loop {
                    let url = format!("{}/events?client_id={}", base_url, client_id_str);
                    let mut es = EventSource::get(&url);

                    tracing::info!("Connecting to server SSE stream: {}", url);

                    while let Some(event_result) = es.next().await {
                        match event_result {
                            Ok(Event::Open) => {
                                tracing::info!("SSE connection opened");
                            }
                            Ok(Event::Message(msg)) => {
                                eprintln!(
                                    "DEBUG [client SSE]: Received message: {}",
                                    &msg.data[..std::cmp::min(200, msg.data.len())]
                                );
                                // Parse the server event from JSON data
                                match serde_json::from_str::<ServerEvent>(&msg.data) {
                                    Ok(server_event) => {
                                        eprintln!(
                                            "DEBUG [client SSE]: Parsed event: {:?}",
                                            server_event.variant_name()
                                        );
                                        if let Err(e) =
                                            handle_server_event(server_event, &runtime_for_sse)
                                        {
                                            tracing::warn!("Error handling server event: {}", e);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to parse server event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("SSE error: {}", e);
                                es.close();
                                break; // Exit inner loop to reconnect
                            }
                        }
                    }

                    // Reconnect after delay
                    tracing::info!("SSE disconnected, reconnecting in 5s...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }))
        } else {
            None
        };

        Ok(Self {
            runtime,
            server_connection,
            context,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            subscription_senders,
            next_handle: std::sync::atomic::AtomicU64::new(1),
            sse_listener_task,
        })
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        self.subscribe_internal(query, None).await
    }

    /// Internal subscribe with optional session.
    async fn subscribe_internal(
        &self,
        query: Query,
        session: Option<Session>,
    ) -> Result<SubscriptionStream> {
        let handle = SubscriptionHandle(
            self.next_handle
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        // Create channel for this subscription's deltas
        let (tx, rx) = mpsc::channel::<RowDelta>(64);

        // Store sender before subscribing so callback can find it
        let senders = self.subscription_senders.clone();

        // Register with runtime using callback pattern
        // The callback bridges runtime updates to the channel
        let runtime_handle = self
            .runtime
            .subscribe(
                query.clone(),
                move |delta| {
                    // Route delta to the subscription's channel
                    // Note: We need to use try_send since we're in a sync callback
                    if let Ok(senders_guard) = senders.try_read() {
                        if let Some(sender) = senders_guard.get(&delta.handle) {
                            let _ = sender.try_send(delta.delta);
                        }
                    }
                },
                session,
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;

        // Register sender for this subscription
        {
            let mut senders = self.subscription_senders.write().await;
            senders.insert(runtime_handle, tx);
        }

        // Subscriptions now flow through outbox automatically via subscribe_with_sync.
        // The RuntimeCore.subscribe() -> QueryManager.subscribe_with_sync() path
        // sends QuerySubscription to connected servers via the outbox.

        // Track subscription metadata
        {
            let mut subs = self.subscriptions.write().await;
            subs.insert(
                handle,
                SubscriptionState {
                    query,
                    runtime_handle,
                    server_query_id: None, // Server query ID not tracked separately anymore
                },
            );
        }

        Ok(SubscriptionStream::new(handle, rx))
    }

    /// One-shot query.
    ///
    /// Returns the current results as `Vec<(ObjectId, Vec<Value>)>`.
    pub async fn query(&self, query: Query) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        let future = self
            .runtime
            .query(query, None)
            .map_err(|e| JazzError::Query(e.to_string()))?;
        future
            .await
            .map_err(|e| JazzError::Query(format!("{:?}", e)))
    }

    /// Create a new row in a table.
    pub async fn create(&self, table: &str, values: Vec<Value>) -> Result<ObjectId> {
        self.runtime
            .insert(table, values, None)
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Update a row.
    pub async fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<()> {
        self.runtime
            .update(object_id, updates, None)
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Delete a row.
    pub async fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.runtime
            .delete(object_id, None)
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        if let Some(state) = subs.remove(&handle) {
            // Remove sender
            let mut senders = self.subscription_senders.write().await;
            senders.remove(&state.runtime_handle);
            // Unsubscribe from runtime
            let _ = self.runtime.unsubscribe(state.runtime_handle);
        }
        Ok(())
    }

    /// Get the current schema.
    pub async fn schema(&self) -> Result<groove::query_manager::types::Schema> {
        self.runtime
            .current_schema()
            .map_err(|e| JazzError::Query(e.to_string()))
    }

    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        self.server_connection.is_some()
    }

    /// Create a session-scoped client for backend operations.
    ///
    /// This allows backend applications to perform operations as a specific user.
    /// The session is used for policy evaluation on all operations.
    ///
    /// Operations go through the local runtime and sync to servers via the outbox,
    /// just like regular operations. The session context flows with the data.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let user_session = Session::new("user-123");
    /// let user_client = client.for_session(user_session);
    /// let id = user_client.create("todos", vec![Value::Text("Buy milk".into())]).await?;
    /// ```
    pub fn for_session(&self, session: Session) -> SessionClient<'_> {
        SessionClient {
            client: self,
            session,
        }
    }

    /// Shutdown the client and release resources.
    ///
    /// Aborts background tasks and flushes the runtime.
    pub async fn shutdown(mut self) -> Result<()> {
        // Abort SSE listener first (it holds TokioRuntime clone)
        if let Some(handle) = self.sse_listener_task.take() {
            handle.abort();
            // Wait for abort to complete (ignore JoinError::Cancelled)
            let _ = handle.await;
        }

        // Flush pending operations
        self.runtime
            .flush()
            .await
            .map_err(|e| JazzError::Connection(e.to_string()))?;

        Ok(())
    }
}

/// Session-scoped client for backend operations.
///
/// Created by `JazzClient::for_session()`. Allows backend applications
/// to perform operations as a specific user.
///
/// Operations go through the local runtime with the session context,
/// then sync to the server via the outbox (same as regular operations).
/// The session is used for policy evaluation.
pub struct SessionClient<'a> {
    client: &'a JazzClient,
    session: Session,
}

impl<'a> SessionClient<'a> {
    /// Create a new row in a table as this session's user.
    ///
    /// The session is used for policy evaluation. Changes sync to server
    /// via the outbox with ObjectUpdated payloads.
    pub async fn create(&self, table: &str, values: Vec<Value>) -> Result<ObjectId> {
        self.client
            .runtime
            .insert(table, values, Some(&self.session))
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Update a row as this session's user.
    ///
    /// The session is used for policy evaluation.
    pub async fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<()> {
        self.client
            .runtime
            .update(object_id, updates, Some(&self.session))
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Delete a row as this session's user.
    ///
    /// The session is used for policy evaluation.
    pub async fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.client
            .runtime
            .delete(object_id, Some(&self.session))
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Query as this session's user.
    ///
    /// The session is used for policy evaluation (row-level filtering).
    pub async fn query(&self, query: Query) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        let future = self
            .client
            .runtime
            .query(query, Some(self.session.clone()))
            .map_err(|e| JazzError::Query(e.to_string()))?;
        future
            .await
            .map_err(|e| JazzError::Query(format!("{:?}", e)))
    }

    /// Subscribe to a query as this session's user.
    ///
    /// The session is used for policy evaluation (row-level filtering).
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        self.client
            .subscribe_internal(query, Some(self.session.clone()))
            .await
    }
}

/// Handle incoming server events.
fn handle_server_event(event: ServerEvent, runtime: &TokioRuntime) -> Result<()> {
    match event {
        ServerEvent::Connected {
            connection_id,
            client_id,
        } => {
            tracing::info!(
                "SSE connected with id: {:?}, client_id: {}",
                connection_id,
                client_id
            );
            Ok(())
        }
        ServerEvent::SyncUpdate { payload } => {
            // Push to local runtime inbox
            let entry = InboxEntry {
                source: Source::Server(ServerId::default()),
                payload,
            };
            runtime
                .push_sync_inbox(entry)
                .map_err(|e| JazzError::Sync(e.to_string()))?;
            Ok(())
        }
        ServerEvent::Subscribed { query_id } => {
            tracing::debug!("Server acknowledged subscription: {:?}", query_id);
            Ok(())
        }
        ServerEvent::Error { message, code } => {
            tracing::error!("Server error {:?}: {}", code, message);
            Ok(())
        }
        ServerEvent::Heartbeat => {
            tracing::trace!("Heartbeat received");
            Ok(())
        }
    }
}

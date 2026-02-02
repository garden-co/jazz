//! JazzClient implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use groove::query_manager::query::Query;
use groove::query_manager::types::{RowDelta, SchemaHash, Value};
use groove::schema_manager::{QuerySchemaContext, SchemaManager};
use groove::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncManager};
use groove_rocksdb::RocksDbDriver;
use groove_tokio::{JazzRuntime, RuntimeHandle, SubscriptionHandle as RuntimeSubHandle};
use jazz_transport::{ServerEvent, SubscribeRequest};
use reqwest_eventsource::{Event, EventSource};
use tokio::sync::{RwLock, mpsc};

use crate::transport::ServerConnection;
use crate::{AppContext, JazzError, ObjectId, Result, SubscriptionHandle, SubscriptionStream};

/// Jazz client for building applications.
///
/// Combines local persistence with server sync.
pub struct JazzClient {
    /// Handle to the local runtime.
    runtime_handle: RuntimeHandle,
    /// Connection to the server (shared for event processor).
    server_connection: Option<Arc<ServerConnection>>,
    /// This client's unique ID.
    client_id: ClientId,
    /// Client configuration.
    #[allow(dead_code)]
    context: AppContext,
    /// Schema hash for this client (used in server subscriptions).
    schema_hash: groove::query_manager::types::SchemaHash,
    /// Environment name (used in server subscriptions).
    env: String,
    /// User-facing branch name (used in server subscriptions).
    user_branch: String,
    /// Active subscriptions (metadata).
    subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, SubscriptionState>>>,
    /// Subscription delta senders (shared with event processor).
    subscription_senders: Arc<RwLock<HashMap<RuntimeSubHandle, mpsc::Sender<RowDelta>>>>,
    /// Next subscription handle ID.
    next_handle: std::sync::atomic::AtomicU64,
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

        // Open RocksDB
        let rocksdb_path = context.data_dir.join("rocksdb");
        let driver =
            RocksDbDriver::open(&rocksdb_path).map_err(|e| JazzError::Storage(e.to_string()))?;

        // Create managers
        let sync_manager = SyncManager::new();
        let mut schema_manager = SchemaManager::new(
            sync_manager,
            context.schema.clone(),
            context.app_id,
            "client",
            "main",
        )
        .map_err(|e| JazzError::Schema(format!("{:?}", e)))?;

        // Persist schema to catalogue for server sync
        schema_manager.persist_schema();

        // Create runtime (no separate task needed - scheduling is implicit)
        let (runtime_handle, mut events) = JazzRuntime::new(schema_manager, driver);

        // Connect to server if URL provided (before spawning event processor)
        let server_connection = if !context.server_url.is_empty() {
            match ServerConnection::connect(&context.server_url).await {
                Ok(conn) => {
                    // Register server with sync manager - this queues all local objects for sync
                    let server_id = ServerId::default();
                    if let Err(e) = runtime_handle.add_server(server_id).await {
                        tracing::warn!("Failed to register server with sync manager: {}", e);
                    }
                    Some(Arc::new(conn))
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to server: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Create shared subscription senders map
        let subscription_senders: Arc<RwLock<HashMap<RuntimeSubHandle, mpsc::Sender<RowDelta>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let senders_for_processor = subscription_senders.clone();

        // Clone server connection for event processor
        let server_conn_for_processor = server_connection.clone();
        let client_id_for_processor = client_id;

        // Spawn event processor
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    groove_tokio::RuntimeEvent::SyncOutbox(entry) => {
                        // Send to server if connected and destination is server
                        if let Destination::Server(_) = entry.destination {
                            if let Some(ref conn) = server_conn_for_processor {
                                let conn = conn.clone();
                                let payload = entry.payload.clone();
                                let cid = client_id_for_processor;
                                tokio::spawn(async move {
                                    if let Err(e) = conn.push_sync(payload, cid).await {
                                        tracing::warn!("Failed to push sync to server: {}", e);
                                    }
                                });
                            }
                        }
                    }
                    groove_tokio::RuntimeEvent::SubscriptionUpdate { handle, delta } => {
                        // Route delta to the subscription's channel
                        let senders = senders_for_processor.read().await;
                        if let Some(sender) = senders.get(&handle) {
                            let _ = sender.send(delta).await;
                        }
                    }
                }
            }
        });

        // Spawn SSE listener if connected to server
        if let Some(ref conn) = server_connection {
            let base_url = conn.base_url().to_string();
            let client_id_str = client_id.to_string();
            let runtime_handle_for_sse = runtime_handle.clone();

            tokio::spawn(async move {
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
                                // Parse the server event from JSON data
                                match serde_json::from_str::<ServerEvent>(&msg.data) {
                                    Ok(server_event) => {
                                        if let Err(e) = handle_server_event(
                                            server_event,
                                            &runtime_handle_for_sse,
                                        )
                                        .await
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
            });
        }

        // Compute schema hash for server subscriptions
        let schema_hash = SchemaHash::compute(&context.schema);
        let env = "client".to_string();
        let user_branch = "main".to_string();

        Ok(Self {
            runtime_handle,
            server_connection,
            client_id,
            context,
            schema_hash,
            env,
            user_branch,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            subscription_senders,
            next_handle: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        let handle = SubscriptionHandle(
            self.next_handle
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        // Register with runtime
        let runtime_handle = self
            .runtime_handle
            .subscribe(query.clone(), None)
            .await
            .map_err(|e| JazzError::Query(e.to_string()))?;

        // Create channel for this subscription's deltas
        let (tx, rx) = mpsc::channel::<RowDelta>(64);

        // Register sender with event processor
        {
            let mut senders = self.subscription_senders.write().await;
            senders.insert(runtime_handle, tx);
        }

        // Subscribe to server for sync
        let server_query_id = if let Some(conn) = &self.server_connection {
            let schema_context =
                QuerySchemaContext::new(&self.env, self.schema_hash, &self.user_branch);
            let request = SubscribeRequest {
                query: query.clone(),
                schema_context,
                session: None,
            };
            match conn.subscribe(request).await {
                Ok(response) => Some(response.query_id),
                Err(e) => {
                    tracing::warn!("Failed to subscribe to server: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Track subscription metadata
        {
            let mut subs = self.subscriptions.write().await;
            subs.insert(
                handle,
                SubscriptionState {
                    query,
                    runtime_handle,
                    server_query_id,
                },
            );
        }

        Ok(SubscriptionStream::new(handle, rx))
    }

    /// One-shot query.
    ///
    /// Returns the current results as `Vec<(ObjectId, Vec<Value>)>`.
    pub async fn query(&self, query: Query) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        self.runtime_handle
            .query(query, None)
            .await
            .map_err(|e| JazzError::Query(e.to_string()))
    }

    /// Create a new row in a table.
    pub async fn create(&self, table: &str, values: Vec<Value>) -> Result<ObjectId> {
        self.runtime_handle
            .insert(table, values, None)
            .await
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Update a row.
    pub async fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<()> {
        self.runtime_handle
            .update(object_id, updates, None)
            .await
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Delete a row.
    pub async fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.runtime_handle
            .delete(object_id, None)
            .await
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        if let Some(_state) = subs.remove(&handle) {
            // TODO: Unsubscribe from server
            // TODO: Unsubscribe locally
        }
        Ok(())
    }

    /// Get the current schema.
    pub async fn schema(&self) -> Result<groove::query_manager::types::Schema> {
        self.runtime_handle
            .get_schema()
            .await
            .map_err(|e| JazzError::Query(e.to_string()))
    }

    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        self.server_connection.is_some()
    }

    /// Shutdown the client and release resources.
    ///
    /// In the new design, there's no separate runtime task.
    /// The RuntimeHandle's shutdown is a no-op since scheduling is implicit.
    pub async fn shutdown(self) -> Result<()> {
        // Signal shutdown (no-op in new design)
        self.runtime_handle
            .shutdown()
            .await
            .map_err(|e| JazzError::Connection(e.to_string()))?;

        Ok(())
    }
}

/// Handle incoming server events.
async fn handle_server_event(event: ServerEvent, runtime_handle: &RuntimeHandle) -> Result<()> {
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
            runtime_handle
                .push_sync_inbox(entry)
                .await
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

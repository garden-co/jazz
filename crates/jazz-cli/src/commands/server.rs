//! Server command implementation.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use groove::schema_manager::{AppId, SchemaManager};
use groove::sync_manager::{ClientId, Destination, SyncManager, SyncPayload};
use groove_rocksdb::RocksDbDriver;
use groove_tokio::{JazzRuntime, RuntimeHandle};
use tokio::sync::{RwLock, broadcast};
use tracing::info;

use crate::routes;

/// Server state shared across request handlers.
pub struct ServerState {
    pub runtime_handle: RuntimeHandle,
    #[allow(dead_code)]
    pub app_id: AppId,
    pub connections: RwLock<HashMap<u64, ConnectionState>>,
    pub next_connection_id: std::sync::atomic::AtomicU64,
    /// Broadcast channel for sending sync payloads to SSE clients
    pub sync_broadcast: broadcast::Sender<(ClientId, SyncPayload)>,
}

/// State for a single SSE connection.
pub struct ConnectionState {
    pub client_id: ClientId,
}

/// Run the Jazz server.
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse app ID
    let app_id = AppId::from_string(app_id_str)?;

    info!("Starting Jazz server for app: {}", app_id);
    info!("Data directory: {}", data_dir);

    // Create data directory if it doesn't exist
    std::fs::create_dir_all(data_dir)?;

    // Open RocksDB
    let rocksdb_path = format!("{}/rocksdb", data_dir);
    let driver = RocksDbDriver::open(&rocksdb_path)?;

    // Create managers (server mode - no fixed current schema)
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new_server(sync_manager, app_id, "prod");

    // Create runtime (no separate task needed - scheduling is implicit)
    let (runtime_handle, mut events) = JazzRuntime::new(schema_manager, driver);

    // Create broadcast channel for SSE updates
    let (sync_tx, _) = broadcast::channel::<(ClientId, SyncPayload)>(256);
    let sync_tx_clone = sync_tx.clone();

    // Build server state
    let state = Arc::new(ServerState {
        runtime_handle,
        app_id,
        connections: RwLock::new(HashMap::new()),
        next_connection_id: std::sync::atomic::AtomicU64::new(1),
        sync_broadcast: sync_tx,
    });

    // Spawn event processor (routes sync outbox to connected clients)
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                groove_tokio::RuntimeEvent::SyncOutbox(entry) => {
                    // Route to appropriate client via broadcast
                    if let Destination::Client(client_id) = entry.destination {
                        let _ = sync_tx_clone.send((client_id, entry.payload));
                    }
                    // Server destinations would be handled differently (e.g., HTTP push)
                }
                groove_tokio::RuntimeEvent::SubscriptionUpdate { handle, delta } => {
                    // Subscription updates are typically local to the subscriber
                    // For now, log them - in future could route to specific client
                    tracing::debug!("Subscription update: {:?} delta: {:?}", handle, delta);
                }
            }
        }
    });

    // Build router
    let app = routes::create_router(state);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

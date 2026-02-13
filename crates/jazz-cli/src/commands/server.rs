//! Server command implementation.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use groove::schema_manager::{AppId, SchemaManager};
use groove::storage::RocksDbStorage;
use groove::sync_manager::{ClientId, Destination, PersistenceTier, SyncManager, SyncPayload};
use groove_tokio::TokioRuntime;
use tokio::sync::{RwLock, broadcast};
use tracing::info;

use crate::middleware::AuthConfig;
use crate::routes;

/// Server state shared across request handlers.
pub struct ServerState {
    pub runtime: TokioRuntime<RocksDbStorage>,
    #[allow(dead_code)]
    pub app_id: AppId,
    pub connections: RwLock<HashMap<u64, ConnectionState>>,
    pub next_connection_id: AtomicU64,
    /// Pending delayed client cleanup tasks (for reconnect grace window)
    pub pending_client_cleanup: RwLock<HashMap<ClientId, PendingClientCleanup>>,
    pub next_cleanup_generation: AtomicU64,
    pub client_disconnect_grace: Duration,
    /// Broadcast channel for sending sync payloads to SSE clients
    pub sync_broadcast: broadcast::Sender<(ClientId, SyncPayload)>,
    /// Authentication configuration
    pub auth_config: AuthConfig,
}

/// State for a single SSE connection.
pub struct ConnectionState {
    pub client_id: ClientId,
}

pub struct PendingClientCleanup {
    pub generation: u64,
    pub handle: tokio::task::JoinHandle<()>,
}

/// Run the Jazz server.
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    auth_config: AuthConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse app ID
    let app_id = AppId::from_string(app_id_str)?;

    info!("Starting Jazz server for app: {}", app_id);
    info!("Data directory: {}", data_dir);

    // Create data directory if it doesn't exist
    std::fs::create_dir_all(data_dir)?;

    // Create managers (server mode - no fixed current schema)
    let sync_manager = SyncManager::new().with_tier(PersistenceTier::EdgeServer);
    let schema_manager = SchemaManager::new_server(sync_manager, app_id, "prod");

    // Create broadcast channel for SSE updates
    let (sync_tx, _) = broadcast::channel::<(ClientId, SyncPayload)>(256);
    let sync_tx_clone = sync_tx.clone();

    // Create persistent storage
    let db_path = format!("{}/groove.rocksdb", data_dir);
    let storage = RocksDbStorage::open(&db_path, 64 * 1024 * 1024)
        .map_err(|e| format!("Failed to open storage: {:?}", e))?;

    // Create runtime with sync callback that routes to SSE clients
    let runtime = TokioRuntime::new(schema_manager, storage, move |entry| {
        // Route to appropriate client via broadcast
        if let Destination::Client(client_id) = entry.destination {
            eprintln!(
                "DEBUG [server sync_cb]: Broadcasting {} to client {}",
                entry.payload.variant_name(),
                client_id
            );
            let _ = sync_tx_clone.send((client_id, entry.payload));
        }
        // Server destinations would be handled differently (e.g., HTTP push)
    });

    // Log auth configuration (without revealing secrets)
    if auth_config.is_configured() {
        info!(
            "Auth configured: jwt={}, jwks={}, backend={}, admin={}",
            auth_config.jwt_secret.is_some(),
            auth_config.jwks_url.is_some(),
            auth_config.backend_secret.is_some(),
            auth_config.admin_secret.is_some()
        );
    } else {
        info!("Auth not configured - all endpoints are public");
    }

    // Build server state
    let state = Arc::new(ServerState {
        runtime,
        app_id,
        connections: RwLock::new(HashMap::new()),
        next_connection_id: AtomicU64::new(1),
        pending_client_cleanup: RwLock::new(HashMap::new()),
        next_cleanup_generation: AtomicU64::new(1),
        client_disconnect_grace: Duration::from_secs(60),
        sync_broadcast: sync_tx,
        auth_config,
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

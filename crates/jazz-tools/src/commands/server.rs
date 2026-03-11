//! Server command implementation.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use jazz_tools::query_manager::query::QueryBuilder;
use jazz_tools::query_manager::types::{ColumnType, SchemaBuilder, TableSchema, Value};
use jazz_tools::runtime_core::ReadDurabilityOptions;
use jazz_tools::runtime_tokio::TokioRuntime;
use jazz_tools::schema_manager::{AppId, SchemaManager, rehydrate_schema_manager_from_manifest};
use jazz_tools::storage::FjallStorage;
use jazz_tools::sync_manager::{ClientId, Destination, DurabilityTier, SyncManager, SyncPayload};
use jsonwebtoken::jwk::JwkSet;
use tokio::sync::{RwLock, broadcast};
use tracing::info;

use crate::middleware::AuthConfig;
use crate::routes;

const EXTERNAL_IDENTITIES_TABLE: &str = "external_identities";

#[derive(Debug, Clone)]
pub struct ExternalIdentityRow {
    pub issuer: String,
    pub subject: String,
    pub principal_id: String,
}

/// Persistent storage for external identity -> principal mappings.
pub struct ExternalIdentityStore {
    runtime: TokioRuntime<FjallStorage>,
}

impl ExternalIdentityStore {
    pub fn new(data_dir: &str) -> Result<Self, String> {
        let meta_dir = Path::new(data_dir).join("meta");
        std::fs::create_dir_all(&meta_dir)
            .map_err(|e| format!("failed to create meta dir '{}': {e}", meta_dir.display()))?;

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder(EXTERNAL_IDENTITIES_TABLE)
                    .column("app_id", ColumnType::Uuid)
                    .column("issuer", ColumnType::Text)
                    .column("subject", ColumnType::Text)
                    .column("principal_id", ColumnType::Text)
                    .column("created_at", ColumnType::Timestamp)
                    .column("updated_at", ColumnType::Timestamp),
            )
            .build();

        let sync_manager = SyncManager::new()
            .with_durability_tiers([DurabilityTier::EdgeServer, DurabilityTier::GlobalServer]);
        let schema_manager = SchemaManager::new(
            sync_manager,
            schema,
            AppId::from_name("jazz-tools-meta"),
            "meta",
            "main",
        )
        .map_err(|e| format!("failed to initialize meta schema manager: {e:?}"))?;

        let db_path = meta_dir.join("jazz.fjall");
        let storage = FjallStorage::open(&db_path, 64 * 1024 * 1024)
            .map_err(|e| format!("failed to open meta storage '{}': {e:?}", db_path.display()))?;

        let runtime = TokioRuntime::new(schema_manager, storage, |_entry| {});

        Ok(Self { runtime })
    }

    pub async fn list_external_identities(
        &self,
        app_id: AppId,
    ) -> Result<Vec<ExternalIdentityRow>, String> {
        let query = QueryBuilder::new(EXTERNAL_IDENTITIES_TABLE)
            .filter_eq("app_id", Value::Uuid(app_id.as_object_id()))
            .build();

        let future = self
            .runtime
            .query(query, None, ReadDurabilityOptions::default())
            .map_err(|e| format!("external identity query error: {e}"))?;
        let rows = future
            .await
            .map_err(|e| format!("external identity query await error: {e}"))?;

        rows.into_iter()
            .map(|(_, values)| Self::decode_external_identity_row(&values))
            .collect()
    }

    pub async fn get_external_identity(
        &self,
        app_id: AppId,
        issuer: &str,
        subject: &str,
    ) -> Result<Option<ExternalIdentityRow>, String> {
        let query = QueryBuilder::new(EXTERNAL_IDENTITIES_TABLE)
            .filter_eq("app_id", Value::Uuid(app_id.as_object_id()))
            .filter_eq("issuer", Value::Text(issuer.to_string()))
            .filter_eq("subject", Value::Text(subject.to_string()))
            .build();

        let future = self
            .runtime
            .query(query, None, ReadDurabilityOptions::default())
            .map_err(|e| format!("external identity query error: {e}"))?;
        let mut rows = future
            .await
            .map_err(|e| format!("external identity query await error: {e}"))?;

        if let Some((_object_id, values)) = rows.pop() {
            Ok(Some(Self::decode_external_identity_row(&values)?))
        } else {
            Ok(None)
        }
    }

    pub async fn create_external_identity(
        &self,
        app_id: AppId,
        issuer: &str,
        subject: &str,
        principal_id: &str,
    ) -> Result<(), String> {
        let now = now_timestamp_us();
        let values = vec![
            Value::Uuid(app_id.as_object_id()),
            Value::Text(issuer.to_string()),
            Value::Text(subject.to_string()),
            Value::Text(principal_id.to_string()),
            Value::Timestamp(now),
            Value::Timestamp(now),
        ];

        self.runtime
            .insert(EXTERNAL_IDENTITIES_TABLE, values, None)
            .map_err(|e| format!("failed to insert external identity: {e}"))?;
        Ok(())
    }

    fn decode_external_identity_row(values: &[Value]) -> Result<ExternalIdentityRow, String> {
        if values.len() < 6 {
            return Err(format!(
                "external identity row has invalid column count: expected at least 6, got {}",
                values.len()
            ));
        }

        let issuer = match &values[1] {
            Value::Text(s) => s.clone(),
            other => {
                return Err(format!(
                    "external identity field issuer expected text, got {other:?}"
                ));
            }
        };

        let subject = match &values[2] {
            Value::Text(s) => s.clone(),
            other => {
                return Err(format!(
                    "external identity field subject expected text, got {other:?}"
                ));
            }
        };

        let principal_id = match &values[3] {
            Value::Text(s) => s.clone(),
            other => {
                return Err(format!(
                    "external identity field principal_id expected text, got {other:?}"
                ));
            }
        };

        Ok(ExternalIdentityRow {
            issuer,
            subject,
            principal_id,
        })
    }
}

/// Server state shared across request handlers.
pub struct ServerState {
    pub runtime: TokioRuntime<FjallStorage>,
    #[allow(dead_code)]
    pub app_id: AppId,
    pub connections: RwLock<HashMap<u64, ConnectionState>>,
    pub next_connection_id: std::sync::atomic::AtomicU64,
    /// Broadcast channel for sending sync payloads to SSE clients
    pub sync_broadcast: broadcast::Sender<(ClientId, SyncPayload)>,
    /// Authentication configuration
    pub auth_config: AuthConfig,
    /// Persistent external identity mapping store.
    pub external_identity_store: Arc<ExternalIdentityStore>,
    /// In-memory cache: (issuer, subject) -> principal_id.
    pub external_identities: RwLock<HashMap<(String, String), String>>,
}

/// State for a single SSE connection.
pub struct ConnectionState {
    pub _client_id: ClientId,
}

/// Run the Jazz server.
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    mut auth_config: AuthConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse app ID
    let app_id = AppId::from_string(app_id_str)?;

    info!("Starting Jazz server for app: {}", app_id);
    info!("Data directory: {}", data_dir);

    // Create data directory if it doesn't exist
    std::fs::create_dir_all(data_dir)?;

    // Create managers (server mode - no fixed current schema)
    let sync_manager = SyncManager::new()
        .with_durability_tiers([DurabilityTier::EdgeServer, DurabilityTier::GlobalServer]);
    let mut schema_manager = SchemaManager::new_server(sync_manager, app_id, "prod");

    // Create broadcast channel for SSE updates
    let (sync_tx, _) = broadcast::channel::<(ClientId, SyncPayload)>(256);
    let sync_tx_clone = sync_tx.clone();

    // Create persistent storage
    let db_path = format!("{}/jazz.fjall", data_dir);
    let storage = FjallStorage::open(&db_path, 64 * 1024 * 1024)
        .map_err(|e| format!("Failed to open storage: {:?}", e))?;

    rehydrate_schema_manager_from_manifest(&mut schema_manager, &storage, app_id)
        .map_err(|e| format!("failed to rehydrate schema manager: {e}"))?;

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

    // Preload JWKS when configured.
    if let Some(jwks_url) = &auth_config.jwks_url {
        let jwks = reqwest::get(jwks_url).await?.json::<JwkSet>().await?;
        auth_config.jwks_set = Some(jwks);
    }

    // Log auth configuration (without revealing secrets)
    if auth_config.is_configured() {
        info!(
            "Auth configured: anonymous={}, demo={}, jwks={}, backend={}, admin={}",
            auth_config.allow_anonymous,
            auth_config.allow_demo,
            auth_config.jwks_url.is_some(),
            auth_config.backend_secret.is_some(),
            auth_config.admin_secret.is_some()
        );
    } else {
        info!(
            "Auth configured: anonymous={}, demo={}, jwks=false, backend=false, admin=false",
            auth_config.allow_anonymous, auth_config.allow_demo
        );
    }

    let external_identity_store = Arc::new(
        ExternalIdentityStore::new(data_dir)
            .map_err(|e| format!("failed to initialize external identity store: {e}"))?,
    );
    let external_identity_rows = external_identity_store
        .list_external_identities(app_id)
        .await
        .map_err(|e| format!("failed to load external identities: {e}"))?;
    let mut external_identities = HashMap::with_capacity(external_identity_rows.len());
    for row in external_identity_rows {
        external_identities.insert((row.issuer, row.subject), row.principal_id);
    }

    // Build server state
    let state = Arc::new(ServerState {
        runtime,
        app_id,
        connections: RwLock::new(HashMap::new()),
        next_connection_id: std::sync::atomic::AtomicU64::new(1),
        sync_broadcast: sync_tx,
        auth_config,
        external_identity_store,
        external_identities: RwLock::new(external_identities),
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

fn now_timestamp_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros().min(u128::from(u64::MAX)) as u64,
        Err(_) => 0,
    }
}

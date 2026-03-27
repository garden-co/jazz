use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use axum::Router;
use tokio::sync::{RwLock, broadcast};
use tracing::info;

use crate::middleware::AuthConfig;
use crate::middleware::auth::{JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache};
use crate::query_manager::types::Schema;
use crate::routes;
use crate::runtime_tokio::TokioRuntime;
use crate::schema_manager::{AppId, SchemaManager, rehydrate_schema_manager_from_manifest};
use crate::server::{DynStorage, ExternalIdentityStore, ServerState};
use crate::storage::{FjallStorage, MemoryStorage, Storage};
use crate::sync_manager::{ClientId, Destination, DurabilityTier, SyncManager, SyncPayload};

const STORAGE_CACHE_SIZE_BYTES: usize = 64 * 1024 * 1024;
const SYNC_BROADCAST_CAPACITY: usize = 256;

pub struct BuiltServer {
    #[cfg_attr(not(test), allow(dead_code))]
    pub state: Arc<ServerState>,
    pub app: Router,
}

#[cfg_attr(not(test), allow(dead_code))]
enum ServerSchemaMode {
    Dynamic,
    Fixed(Schema),
}

enum ServerStorageMode {
    Persistent { data_dir: String },
    InMemory,
}

pub struct ServerBuilder {
    app_id: AppId,
    auth_config: AuthConfig,
    schema_mode: ServerSchemaMode,
    storage_mode: ServerStorageMode,
}

impl ServerBuilder {
    pub fn new(app_id: AppId) -> Self {
        Self {
            app_id,
            auth_config: AuthConfig::default(),
            schema_mode: ServerSchemaMode::Dynamic,
            storage_mode: ServerStorageMode::Persistent {
                data_dir: "./data".to_string(),
            },
        }
    }

    pub fn with_auth_config(mut self, auth_config: AuthConfig) -> Self {
        self.auth_config = auth_config;
        self
    }

    pub fn with_persistent_storage(mut self, data_dir: impl Into<String>) -> Self {
        self.storage_mode = ServerStorageMode::Persistent {
            data_dir: data_dir.into(),
        };
        self
    }

    pub fn with_in_memory_storage(mut self) -> Self {
        self.storage_mode = ServerStorageMode::InMemory;
        self
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema_mode = ServerSchemaMode::Fixed(schema);
        self
    }

    pub async fn build(self) -> Result<BuiltServer, String> {
        let auth_config = self.auth_config.clone();
        let jwks_cache = build_jwks_cache(&auth_config).await?;
        log_auth_config(&auth_config);

        let (runtime, sync_broadcast) = self.build_runtime()?;
        let external_identity_store = Arc::new(self.build_external_identity_store()?);
        let external_identity_rows = external_identity_store
            .list_external_identities(self.app_id)
            .await
            .map_err(|e| format!("failed to load external identities: {e}"))?;

        let mut external_identities = HashMap::with_capacity(external_identity_rows.len());
        for row in external_identity_rows {
            external_identities.insert((row.issuer, row.subject), row.principal_id);
        }

        let state = Arc::new(ServerState {
            runtime,
            app_id: self.app_id,
            connections: RwLock::new(HashMap::new()),
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
            sync_broadcast,
            auth_config,
            jwks_cache,
            external_identity_store,
            external_identities: RwLock::new(external_identities),
            disconnect_candidates: RwLock::new(HashMap::new()),
            client_ttl: AtomicU64::new(300_000),
        });

        // Spawn periodic client state sweep (uses Weak so the task exits
        // when all strong refs to ServerState are dropped, e.g. in tests).
        {
            let weak_state = Arc::downgrade(&state);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    let Some(state) = weak_state.upgrade() else {
                        break;
                    };
                    let reaped = state.run_sweep_once().await;
                    if !reaped.is_empty() {
                        tracing::info!(count = reaped.len(), "reaped stale disconnected clients");
                    }
                }
            });
        }

        let app = routes::create_router(state.clone());
        Ok(BuiltServer { state, app })
    }

    #[allow(clippy::type_complexity)]
    fn build_runtime(
        &self,
    ) -> Result<
        (
            TokioRuntime<DynStorage>,
            broadcast::Sender<(ClientId, SyncPayload)>,
        ),
        String,
    > {
        let (sync_tx, _) = broadcast::channel::<(ClientId, SyncPayload)>(SYNC_BROADCAST_CAPACITY);
        let sync_tx_clone = sync_tx.clone();

        let storage = self.build_main_storage()?;
        let schema_manager = self.build_schema_manager(storage.as_ref())?;
        let runtime = TokioRuntime::new(schema_manager, storage, move |entry| {
            if let Destination::Client(client_id) = entry.destination {
                let _ = sync_tx_clone.send((client_id, entry.payload));
            }
        });

        Ok((runtime, sync_tx))
    }

    fn build_schema_manager(&self, storage: &dyn Storage) -> Result<SchemaManager, String> {
        let sync_manager = server_sync_manager();

        match &self.schema_mode {
            ServerSchemaMode::Dynamic => {
                let mut schema_manager =
                    SchemaManager::new_server(sync_manager, self.app_id, "prod");
                rehydrate_schema_manager_from_manifest(&mut schema_manager, storage, self.app_id)
                    .map_err(|e| format!("failed to rehydrate schema manager: {e}"))?;
                Ok(schema_manager)
            }
            ServerSchemaMode::Fixed(schema) => {
                SchemaManager::new(sync_manager, schema.clone(), self.app_id, "prod", "main")
                    .map_err(|e| format!("failed to initialize schema manager: {e:?}"))
            }
        }
    }

    fn build_main_storage(&self) -> Result<DynStorage, String> {
        match &self.storage_mode {
            ServerStorageMode::Persistent { data_dir } => {
                std::fs::create_dir_all(data_dir)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", data_dir))?;

                let db_path = Path::new(data_dir).join("jazz.fjall");
                let storage =
                    FjallStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
                        format!("failed to open storage '{}': {e:?}", db_path.display())
                    })?;
                Ok(Box::new(storage))
            }
            ServerStorageMode::InMemory => Ok(Box::new(MemoryStorage::new())),
        }
    }

    fn build_external_identity_store(&self) -> Result<ExternalIdentityStore, String> {
        match &self.storage_mode {
            ServerStorageMode::Persistent { data_dir } => {
                let meta_dir = Path::new(data_dir).join("meta");
                std::fs::create_dir_all(&meta_dir).map_err(|e| {
                    format!("failed to create meta dir '{}': {e}", meta_dir.display())
                })?;

                let db_path = meta_dir.join("jazz.fjall");
                let storage =
                    FjallStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
                        format!("failed to open meta storage '{}': {e:?}", db_path.display())
                    })?;
                ExternalIdentityStore::new_with_storage(Box::new(storage))
            }
            ServerStorageMode::InMemory => {
                ExternalIdentityStore::new_with_storage(Box::new(MemoryStorage::new()))
            }
        }
    }
}

fn server_sync_manager() -> SyncManager {
    SyncManager::new()
        .with_durability_tiers([DurabilityTier::EdgeServer, DurabilityTier::GlobalServer])
}

async fn build_jwks_cache(auth_config: &AuthConfig) -> Result<Option<JwksCache>, String> {
    let Some(jwks_url) = auth_config.jwks_url.as_ref() else {
        return Ok(None);
    };

    let jwks_ttl = std::env::var("JAZZ_JWKS_CACHE_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(JWKS_CACHE_TTL);
    let jwks_max_stale = std::env::var("JAZZ_JWKS_MAX_STALE_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(JWKS_MAX_STALE);

    let cache = JwksCache::new(
        jwks_url.clone(),
        reqwest::Client::new(),
        jwks_ttl,
        jwks_max_stale,
    );
    cache
        .load(false)
        .await
        .map_err(|e| format!("failed to fetch initial JWKS: {e}"))?;

    Ok(Some(cache))
}

fn log_auth_config(auth_config: &AuthConfig) {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_builder_creates_working_external_identity_store() {
        let app_id = AppId::from_name("builder-test-app");
        let built = ServerBuilder::new(app_id)
            .with_in_memory_storage()
            .build()
            .await
            .expect("build server");

        built
            .state
            .external_identity_store
            .create_external_identity(
                app_id,
                "https://issuer.example",
                "subject-123",
                "principal-123",
            )
            .await
            .expect("create external identity");

        let existing = built
            .state
            .external_identity_store
            .get_external_identity(app_id, "https://issuer.example", "subject-123")
            .await
            .expect("query external identity");

        assert!(existing.is_some());
    }
}

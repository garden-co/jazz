use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use tokio::sync::RwLock;
use tracing::info;

use crate::middleware::AuthConfig;
use crate::middleware::auth::{JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache};
use crate::query_manager::types::Schema;
use crate::routes;
use crate::runtime_tokio::TokioRuntime;
use crate::schema_manager::{AppId, SchemaManager, rehydrate_schema_manager_from_catalogue};
use crate::server::{
    CatalogueAuthorityMode, ConnectionEventHub, DynStorage, ExternalIdentityStore, ServerState,
};
#[cfg(feature = "rocksdb")]
use crate::storage::RocksDBStorage;
#[cfg(feature = "sqlite")]
use crate::storage::SqliteStorage;
use crate::storage::{MemoryStorage, Storage};
use crate::sync_manager::{Destination, DurabilityTier, SyncManager};

#[cfg(feature = "rocksdb")]
const STORAGE_CACHE_SIZE_BYTES: usize = 64 * 1024 * 1024;

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
    Persistent {
        data_dir: String,
    },
    /// Explicitly selects SQLite regardless of which other storage features are
    /// enabled.  Created via [`ServerBuilder::with_sqlite_storage`].
    #[cfg(feature = "sqlite")]
    PersistentSqlite {
        data_dir: String,
    },
    /// Explicitly selects RocksDB regardless of which other storage features
    /// are enabled.  Created via [`ServerBuilder::with_rocksdb_storage`].
    #[cfg(feature = "rocksdb")]
    PersistentRocksDb {
        data_dir: String,
    },
    InMemory,
}

pub struct ServerBuilder {
    app_id: AppId,
    auth_config: AuthConfig,
    catalogue_authority: CatalogueAuthorityMode,
    schema_mode: ServerSchemaMode,
    storage_mode: ServerStorageMode,
    sync_tracer: Option<crate::sync_tracer::SyncTracer>,
}

impl ServerBuilder {
    pub fn new(app_id: AppId) -> Self {
        Self {
            app_id,
            auth_config: AuthConfig::default(),
            catalogue_authority: CatalogueAuthorityMode::Local,
            schema_mode: ServerSchemaMode::Dynamic,
            storage_mode: ServerStorageMode::Persistent {
                data_dir: "./data".to_string(),
            },
            sync_tracer: None,
        }
    }

    pub fn with_sync_tracer(mut self, tracer: crate::sync_tracer::SyncTracer) -> Self {
        self.sync_tracer = Some(tracer);
        self
    }

    pub fn with_auth_config(mut self, auth_config: AuthConfig) -> Self {
        self.auth_config = auth_config;
        self
    }

    pub fn with_catalogue_authority(mut self, catalogue_authority: CatalogueAuthorityMode) -> Self {
        self.catalogue_authority = catalogue_authority;
        self
    }

    pub fn with_persistent_storage(mut self, data_dir: impl Into<String>) -> Self {
        self.storage_mode = ServerStorageMode::Persistent {
            data_dir: data_dir.into(),
        };
        self
    }

    /// Use SQLite as the persistent storage backend, regardless of which other
    /// storage features (e.g. `rocksdb`) are enabled.  Prefer this over
    /// [`with_persistent_storage`] whenever you need to pin the backend to
    /// SQLite (e.g. in tests or on mobile).
    #[cfg(feature = "sqlite")]
    pub fn with_sqlite_storage(mut self, data_dir: impl Into<String>) -> Self {
        self.storage_mode = ServerStorageMode::PersistentSqlite {
            data_dir: data_dir.into(),
        };
        self
    }

    /// Use RocksDB as the persistent storage backend, regardless of which
    /// other storage features are enabled.  Prefer this over
    /// [`with_persistent_storage`] whenever you need to pin the backend to
    /// RocksDB (e.g. in tests or on desktop/server deployments).
    #[cfg(feature = "rocksdb")]
    pub fn with_rocksdb_storage(mut self, data_dir: impl Into<String>) -> Self {
        self.storage_mode = ServerStorageMode::PersistentRocksDb {
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
        validate_catalogue_authority(&auth_config, &self.catalogue_authority)?;
        let jwks_cache = build_jwks_cache(&auth_config).await?;
        log_auth_config(&auth_config, &self.catalogue_authority);

        let (runtime, connection_event_hub) = self.build_runtime()?;
        let external_identity_store = Arc::new(self.build_external_identity_store()?);
        let http_client = reqwest::Client::builder()
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;
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
            connection_event_hub,
            auth_config,
            catalogue_authority: self.catalogue_authority.clone(),
            jwks_cache,
            http_client,
            external_identity_store,
            external_identities: RwLock::new(external_identities),
            disconnect_candidates: RwLock::new(HashMap::new()),
            client_ttl: RwLock::new(Duration::from_secs(300)),
            sync_tracer: self.sync_tracer.clone(),
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
    fn build_runtime(&self) -> Result<(TokioRuntime<DynStorage>, Arc<ConnectionEventHub>), String> {
        let connection_event_hub = Arc::new(ConnectionEventHub::default());
        let dispatch_hub = Arc::clone(&connection_event_hub);
        let tracer_for_outgoing = self.sync_tracer.clone();

        let storage = self.build_main_storage()?;
        let schema_manager = self.build_schema_manager(storage.as_ref())?;
        let runtime = TokioRuntime::new(schema_manager, storage, move |entry| {
            if let Destination::Client(client_id) = entry.destination {
                // Record outgoing server message to tracer if present
                if let Some(ref tracer) = tracer_for_outgoing {
                    tracer.record_outgoing("server", &entry.destination, &entry.payload);
                }
                dispatch_hub.dispatch_payload(client_id, entry.payload);
            }
        });

        Ok((runtime, connection_event_hub))
    }

    fn build_schema_manager(&self, storage: &dyn Storage) -> Result<SchemaManager, String> {
        let sync_manager = server_sync_manager();

        match &self.schema_mode {
            ServerSchemaMode::Dynamic => {
                let mut schema_manager =
                    SchemaManager::new_server(sync_manager, self.app_id, "prod");
                rehydrate_schema_manager_from_catalogue(&mut schema_manager, storage, self.app_id)
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

                #[cfg(feature = "rocksdb")]
                {
                    let db_path = Path::new(data_dir).join("jazz.rocksdb");
                    let storage = RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES)
                        .map_err(|e| {
                            format!("failed to open storage '{}': {e:?}", db_path.display())
                        })?;
                    Ok(Box::new(storage))
                }
                #[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
                {
                    let db_path = Path::new(data_dir).join("jazz.sqlite");
                    let storage = SqliteStorage::open(&db_path).map_err(|e| {
                        format!("failed to open storage '{}': {e:?}", db_path.display())
                    })?;
                    Ok(Box::new(storage))
                }
                #[cfg(not(any(feature = "rocksdb", feature = "sqlite")))]
                {
                    Ok(Box::new(MemoryStorage::new()))
                }
            }
            #[cfg(feature = "sqlite")]
            ServerStorageMode::PersistentSqlite { data_dir } => {
                std::fs::create_dir_all(data_dir)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", data_dir))?;
                let db_path = Path::new(data_dir).join("jazz.sqlite");
                let storage = SqliteStorage::open(&db_path).map_err(|e| {
                    format!("failed to open storage '{}': {e:?}", db_path.display())
                })?;
                Ok(Box::new(storage))
            }
            #[cfg(feature = "rocksdb")]
            ServerStorageMode::PersistentRocksDb { data_dir } => {
                std::fs::create_dir_all(data_dir)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", data_dir))?;
                let db_path = Path::new(data_dir).join("jazz.rocksdb");
                let storage =
                    RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
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

                #[cfg(feature = "rocksdb")]
                {
                    let db_path = meta_dir.join("jazz.rocksdb");
                    let storage = RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES)
                        .map_err(|e| {
                            format!("failed to open meta storage '{}': {e:?}", db_path.display())
                        })?;
                    ExternalIdentityStore::new_with_storage(Box::new(storage))
                }
                #[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
                {
                    let db_path = meta_dir.join("jazz.sqlite");
                    let storage = SqliteStorage::open(&db_path).map_err(|e| {
                        format!("failed to open meta storage '{}': {e:?}", db_path.display())
                    })?;
                    ExternalIdentityStore::new_with_storage(Box::new(storage))
                }
                #[cfg(not(any(feature = "rocksdb", feature = "sqlite")))]
                {
                    ExternalIdentityStore::new_with_storage(Box::new(MemoryStorage::new()))
                }
            }
            #[cfg(feature = "sqlite")]
            ServerStorageMode::PersistentSqlite { data_dir } => {
                let meta_dir = Path::new(data_dir).join("meta");
                std::fs::create_dir_all(&meta_dir).map_err(|e| {
                    format!("failed to create meta dir '{}': {e}", meta_dir.display())
                })?;
                let db_path = meta_dir.join("jazz.sqlite");
                let storage = SqliteStorage::open(&db_path).map_err(|e| {
                    format!("failed to open meta storage '{}': {e:?}", db_path.display())
                })?;
                ExternalIdentityStore::new_with_storage(Box::new(storage))
            }
            #[cfg(feature = "rocksdb")]
            ServerStorageMode::PersistentRocksDb { data_dir } => {
                let meta_dir = Path::new(data_dir).join("meta");
                std::fs::create_dir_all(&meta_dir).map_err(|e| {
                    format!("failed to create meta dir '{}': {e}", meta_dir.display())
                })?;
                let db_path = meta_dir.join("jazz.rocksdb");
                let storage =
                    RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
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
    let sync_manager = SyncManager::new()
        .with_durability_tiers([DurabilityTier::EdgeServer, DurabilityTier::GlobalServer]);

    if should_allow_unprivileged_schema_catalogue_writes() {
        sync_manager.with_unprivileged_schema_catalogue_writes()
    } else {
        sync_manager
    }
}

fn should_allow_unprivileged_schema_catalogue_writes() -> bool {
    !matches!(
        std::env::var("NODE_ENV"),
        Ok(value) if value.eq_ignore_ascii_case("production")
    )
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

fn validate_catalogue_authority(
    auth_config: &AuthConfig,
    catalogue_authority: &CatalogueAuthorityMode,
) -> Result<(), String> {
    if matches!(catalogue_authority, CatalogueAuthorityMode::Local) {
        return Ok(());
    }

    if auth_config.admin_secret.is_none() {
        return Err(
            "catalogue authority forwarding requires a local --admin-secret / JAZZ_ADMIN_SECRET"
                .to_string(),
        );
    }

    Ok(())
}

fn log_auth_config(auth_config: &AuthConfig, catalogue_authority: &CatalogueAuthorityMode) {
    let authority_mode = match catalogue_authority {
        CatalogueAuthorityMode::Local => "local".to_string(),
        CatalogueAuthorityMode::Forward { base_url, .. } => {
            format!("forward({base_url})")
        }
    };
    if auth_config.is_configured() {
        info!(
            "Auth configured: anonymous={}, demo={}, jwks={}, backend={}, admin={}, catalogue_authority={}",
            auth_config.allow_anonymous,
            auth_config.allow_demo,
            auth_config.jwks_url.is_some(),
            auth_config.backend_secret.is_some(),
            auth_config.admin_secret.is_some(),
            authority_mode
        );
    } else {
        info!(
            "Auth configured: anonymous={}, demo={}, jwks=false, backend=false, admin=false, catalogue_authority={}",
            auth_config.allow_anonymous, auth_config.allow_demo, authority_mode
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

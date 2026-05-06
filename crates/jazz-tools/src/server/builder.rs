use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use tokio::sync::RwLock;
use tracing::info;

use crate::middleware::AuthConfig;
use crate::middleware::auth::{
    JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache, JwtVerifier, StaticJwtVerifier,
};
use crate::query_manager::types::Schema;
use crate::routes;
use crate::runtime_tokio::TokioRuntime;
use crate::schema_manager::{AppId, SchemaManager, rehydrate_schema_manager_from_catalogue};
use crate::server::{
    CatalogueAuthorityMode, ConnectionEventHub, DynStorage, ServerState, ServerTopology,
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

/// Storage backend selection for [`ServerBuilder::with_storage`].
///
/// `Persistent` picks the best available backend at compile time
/// (RocksDB > SQLite > in-memory). `Sqlite` and `RocksDb` pin the backend
/// regardless of which other storage features are enabled.
#[derive(Debug, Clone)]
pub enum StorageBackend {
    InMemory,
    Persistent {
        path: PathBuf,
    },
    #[cfg(feature = "sqlite")]
    Sqlite {
        path: PathBuf,
    },
    #[cfg(feature = "rocksdb")]
    RocksDb {
        path: PathBuf,
    },
}

pub struct ServerBuilder {
    app_id: AppId,
    auth_config: AuthConfig,
    catalogue_authority: CatalogueAuthorityMode,
    schema_mode: ServerSchemaMode,
    storage_backend: StorageBackend,
    sync_tracer: Option<crate::sync_tracer::SyncTracer>,
    upstream_url: Option<String>,
}

impl ServerBuilder {
    pub fn new(app_id: AppId) -> Self {
        Self {
            app_id,
            auth_config: AuthConfig {
                allow_local_first_auth: true,
                ..Default::default()
            },
            catalogue_authority: CatalogueAuthorityMode::Local,
            schema_mode: ServerSchemaMode::Dynamic,
            storage_backend: StorageBackend::Persistent {
                path: PathBuf::from("./data"),
            },
            sync_tracer: None,
            upstream_url: None,
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

    pub fn with_local_first_auth(mut self, enabled: bool) -> Self {
        self.auth_config.allow_local_first_auth = enabled;
        self
    }

    pub fn with_catalogue_authority(mut self, catalogue_authority: CatalogueAuthorityMode) -> Self {
        self.catalogue_authority = catalogue_authority;
        self
    }

    pub fn with_upstream_url(mut self, upstream_url: impl Into<String>) -> Self {
        self.upstream_url = Some(upstream_url.into());
        self
    }

    pub fn with_storage(mut self, backend: StorageBackend) -> Self {
        self.storage_backend = backend;
        self
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema_mode = ServerSchemaMode::Fixed(schema);
        self
    }

    pub async fn build(self) -> Result<BuiltServer, String> {
        let auth_config = self.auth_config.clone();
        let topology = if self.upstream_url.is_some() {
            ServerTopology::Edge
        } else {
            ServerTopology::Core
        };
        let upstream_ws_url = match self.upstream_url.as_deref() {
            Some(upstream_url) => Some(upstream_ws_url(upstream_url, self.app_id)?),
            None => None,
        };
        validate_server_config(&auth_config, &self.catalogue_authority, topology)?;
        let jwt_verifier = build_jwt_verifier(&auth_config).await?;
        log_auth_config(&auth_config, &self.catalogue_authority, topology);

        let (runtime, connection_event_hub) = self.build_runtime()?;
        if let Some(upstream_ws_url) = upstream_ws_url.clone() {
            start_upstream_sync(&runtime, upstream_ws_url, &auth_config)?;
        }
        let http_client = reqwest::Client::builder()
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;

        let state = Arc::new(ServerState {
            runtime,
            app_id: self.app_id,
            connections: RwLock::new(HashMap::new()),
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
            connection_event_hub,
            auth_config,
            catalogue_authority: self.catalogue_authority.clone(),
            topology,
            jwt_verifier,
            http_client,
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

        let storage = self.build_main_storage()?;
        let schema_manager = self.build_schema_manager(storage.as_ref())?;
        let runtime = TokioRuntime::new(schema_manager, storage, move |entry| {
            if let Destination::Client(client_id) = entry.destination {
                dispatch_hub.dispatch_payload(client_id, entry.payload);
            }
        });

        if let Some(ref tracer) = self.sync_tracer {
            runtime.set_sync_tracer(tracer.clone(), "server".to_string());
        }

        Ok((runtime, connection_event_hub))
    }

    fn build_schema_manager(&self, storage: &dyn Storage) -> Result<SchemaManager, String> {
        let sync_manager = server_sync_manager(self.local_durability_tier());

        match &self.schema_mode {
            ServerSchemaMode::Dynamic => {
                let mut schema_manager =
                    SchemaManager::new_server(sync_manager, self.app_id, "prod");
                rehydrate_schema_manager_from_catalogue(&mut schema_manager, storage, self.app_id)
                    .map_err(|e| format!("failed to rehydrate schema manager: {e}"))?;
                // Dynamic servers fail closed until an explicit permissions head
                // is available for the active app.
                schema_manager
                    .query_manager_mut()
                    .require_authorization_schema();
                Ok(schema_manager)
            }
            ServerSchemaMode::Fixed(schema) => {
                SchemaManager::new(sync_manager, schema.clone(), self.app_id, "prod", "main")
                    .map_err(|e| format!("failed to initialize schema manager: {e:?}"))
            }
        }
    }

    fn build_main_storage(&self) -> Result<DynStorage, String> {
        match &self.storage_backend {
            StorageBackend::Persistent { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;

                #[cfg(feature = "rocksdb")]
                {
                    let db_path = path.join("jazz.rocksdb");
                    let storage = RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES)
                        .map_err(|e| {
                            format!("failed to open storage '{}': {e:?}", db_path.display())
                        })?;
                    Ok(Box::new(storage))
                }
                #[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
                {
                    let db_path = path.join("jazz.sqlite");
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
            StorageBackend::Sqlite { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;
                let db_path = path.join("jazz.sqlite");
                let storage = SqliteStorage::open(&db_path).map_err(|e| {
                    format!("failed to open storage '{}': {e:?}", db_path.display())
                })?;
                Ok(Box::new(storage))
            }
            #[cfg(feature = "rocksdb")]
            StorageBackend::RocksDb { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;
                let db_path = path.join("jazz.rocksdb");
                let storage =
                    RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
                        format!("failed to open storage '{}': {e:?}", db_path.display())
                    })?;
                Ok(Box::new(storage))
            }
            StorageBackend::InMemory => Ok(Box::new(MemoryStorage::new())),
        }
    }

    fn local_durability_tier(&self) -> DurabilityTier {
        if self.upstream_url.is_some() {
            DurabilityTier::EdgeServer
        } else {
            DurabilityTier::GlobalServer
        }
    }
}

fn server_sync_manager(local_tier: DurabilityTier) -> SyncManager {
    let sync_manager = SyncManager::new().with_durability_tier(local_tier);

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

async fn build_jwt_verifier(auth_config: &AuthConfig) -> Result<Option<Arc<JwtVerifier>>, String> {
    match (
        auth_config.jwks_url.as_ref(),
        auth_config.jwt_public_key.as_ref(),
    ) {
        (Some(_), Some(_)) => Err(
            "configure either --jwks-url / JAZZ_JWKS_URL or --jwt-public-key / JAZZ_JWT_PUBLIC_KEY, not both"
                .to_string(),
        ),
        (None, None) => Ok(None),
        (None, Some(public_key)) => {
            let verifier = StaticJwtVerifier::from_public_key(public_key)?;
            Ok(Some(Arc::new(JwtVerifier::Static(verifier))))
        }
        (Some(jwks_url), None) => {
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

            let http_client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| format!("failed to build JWKS HTTP client: {e}"))?;

            let verifier = Arc::new(JwtVerifier::Jwks(JwksCache::new(
                jwks_url.clone(),
                http_client,
                jwks_ttl,
                jwks_max_stale,
            )));

            // Warm the cache in the background. The JWKS endpoint may not be
            // available yet (e.g. Jazz server starts during Next.js config resolution,
            // before the app is listening). First auth request will block on fetch
            // if the background warm hasn't completed.
            {
                let verifier = Arc::clone(&verifier);
                tokio::spawn(async move {
                    if let JwtVerifier::Jwks(cache) = verifier.as_ref()
                        && let Err(e) = cache.load(false).await
                    {
                        tracing::warn!(
                            "Background JWKS warm failed (will retry on first auth request): {e}"
                        );
                    }
                });
            }

            Ok(Some(verifier))
        }
    }
}

fn validate_server_config(
    auth_config: &AuthConfig,
    catalogue_authority: &CatalogueAuthorityMode,
    topology: ServerTopology,
) -> Result<(), String> {
    if topology.is_edge() && auth_config.peer_secret.is_none() {
        return Err("edge mode requires --peer-secret / JAZZ_PEER_SECRET".to_string());
    }

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

fn log_auth_config(
    auth_config: &AuthConfig,
    catalogue_authority: &CatalogueAuthorityMode,
    topology: ServerTopology,
) {
    let authority_mode = match catalogue_authority {
        CatalogueAuthorityMode::Local => "local".to_string(),
        CatalogueAuthorityMode::Forward { base_url, .. } => {
            format!("forward({base_url})")
        }
    };
    info!(
        "Auth configured: local_first={}, jwks={}, static_jwt_key={}, cookie={}, backend={}, admin={}, peer={}, catalogue_authority={}, topology={:?}",
        auth_config.allow_local_first_auth,
        auth_config.jwks_url.is_some(),
        auth_config.jwt_public_key.is_some(),
        auth_config.auth_cookie_name.is_some(),
        auth_config.backend_secret.is_some(),
        auth_config.admin_secret.is_some(),
        auth_config.peer_secret.is_some(),
        authority_mode,
        topology
    );
}

pub fn upstream_ws_url(base_url: &str, app_id: AppId) -> Result<String, String> {
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|err| format!("invalid upstream URL '{base_url}': {err}"))?;

    if url.query().is_some() || url.fragment().is_some() {
        return Err("upstream URL must not include query parameters or a fragment".to_string());
    }

    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" => "ws",
        "wss" => "wss",
        other => {
            return Err(format!(
                "unsupported upstream URL scheme '{other}'; expected http, https, ws, or wss"
            ));
        }
    };
    url.set_scheme(scheme)
        .map_err(|_| format!("failed to set upstream URL scheme to {scheme}"))?;

    let app_ws_path = format!("/apps/{app_id}/ws");
    let normalized_path = url.path().trim_end_matches('/');
    if normalized_path == app_ws_path.trim_end_matches('/') {
        url.set_path(&app_ws_path);
    } else {
        let base_path = match normalized_path {
            "" | "/" => String::new(),
            path => path.to_string(),
        };
        url.set_path(&format!(
            "{}/{}",
            base_path.trim_end_matches('/'),
            app_ws_path.trim_start_matches('/')
        ));
    }

    Ok(url.to_string())
}

fn start_upstream_sync(
    runtime: &TokioRuntime<DynStorage>,
    upstream_ws_url: String,
    auth_config: &AuthConfig,
) -> Result<(), String> {
    let peer_secret = auth_config
        .peer_secret
        .clone()
        .ok_or_else(|| "edge mode requires --peer-secret / JAZZ_PEER_SECRET".to_string())?;

    info!(
        local_tier = "edge",
        upstream_url = %upstream_ws_url,
        upstream_connected = false,
        "starting edge upstream sync"
    );

    runtime.connect(
        upstream_ws_url.clone(),
        crate::transport_manager::AuthConfig {
            peer_secret: Some(peer_secret),
            ..Default::default()
        },
    );

    let wait_runtime = (*runtime).clone();
    tokio::spawn(async move {
        let connected = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            wait_runtime.transport_wait_until_connected(),
        )
        .await
        .unwrap_or(false);
        if connected {
            tracing::info!(
                local_tier = "edge",
                upstream_url = %upstream_ws_url,
                upstream_connected = true,
                "edge upstream sync connected"
            );
        } else {
            tracing::warn!(
                local_tier = "edge",
                upstream_url = %upstream_ws_url,
                upstream_connected = false,
                "edge upstream sync ended before first connection"
            );
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_manager::AppId;

    #[test]
    fn upstream_url_conversion_maps_base_urls_to_app_ws_route() {
        let app_id =
            AppId::from_string("00000000-0000-0000-0000-000000000001").expect("parse app id");

        assert_eq!(
            upstream_ws_url("https://core.example.com", app_id).expect("https conversion"),
            "wss://core.example.com/apps/00000000-0000-0000-0000-000000000001/ws"
        );
        assert_eq!(
            upstream_ws_url("http://core.example.com/base/", app_id).expect("http conversion"),
            "ws://core.example.com/base/apps/00000000-0000-0000-0000-000000000001/ws"
        );
        assert_eq!(
            upstream_ws_url("ws://core.example.com", app_id).expect("ws conversion"),
            "ws://core.example.com/apps/00000000-0000-0000-0000-000000000001/ws"
        );
        assert_eq!(
            upstream_ws_url(
                "wss://core.example.com/apps/00000000-0000-0000-0000-000000000001/ws",
                app_id
            )
            .expect("already app-scoped ws URL"),
            "wss://core.example.com/apps/00000000-0000-0000-0000-000000000001/ws"
        );
    }

    #[test]
    fn upstream_url_conversion_rejects_query_and_fragment_urls() {
        let app_id =
            AppId::from_string("00000000-0000-0000-0000-000000000001").expect("parse app id");

        assert!(upstream_ws_url("https://core.example.com?token=abc", app_id).is_err());
        assert!(upstream_ws_url("https://core.example.com#cluster-a", app_id).is_err());
    }

    #[tokio::test]
    async fn builder_uses_global_tier_without_upstream() {
        let built = ServerBuilder::new(AppId::from_name("global-builder-tier"))
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build global server");

        let tiers = built
            .state
            .runtime
            .with_sync_manager(|sync| sync.local_durability_tiers())
            .expect("read sync manager");

        assert_eq!(
            tiers,
            std::collections::HashSet::from([DurabilityTier::GlobalServer])
        );
    }

    #[tokio::test]
    async fn builder_uses_edge_tier_with_upstream() {
        let built = ServerBuilder::new(AppId::from_name("edge-builder-tier"))
            .with_storage(StorageBackend::InMemory)
            .with_auth_config(AuthConfig {
                peer_secret: Some("cluster-secret".to_string()),
                ..Default::default()
            })
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await
            .expect("build edge server");

        let tiers = built
            .state
            .runtime
            .with_sync_manager(|sync| sync.local_durability_tiers())
            .expect("read sync manager");

        assert_eq!(
            tiers,
            std::collections::HashSet::from([DurabilityTier::EdgeServer])
        );
    }
}

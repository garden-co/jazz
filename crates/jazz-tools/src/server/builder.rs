use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use jazz::schema::JazzSchema;
use jazz_server::StorageConfig;
use tokio::sync::RwLock;
use tracing::info;

use crate::middleware::AuthConfig;
use crate::middleware::auth::{
    JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache, JwtVerifier, StaticJwtVerifier,
};
#[cfg(test)]
use crate::query_manager::types::ComposedBranchName;
use crate::schema_api::Schema;
#[cfg(test)]
use crate::schema_api::SchemaHash;
use crate::schema_manager::AppId;
use crate::server::routes;
use crate::server::{
    ConnectionEventHub, DirectCatalogueStore, DynStorage, ServerState, ServerTopology,
};
use crate::storage::MemoryStorage;
#[cfg(feature = "rocksdb")]
use crate::storage::RocksDBStorage;
#[cfg(feature = "sqlite")]
use crate::storage::SqliteStorage;
#[cfg(test)]
use crate::sync::DurabilityTier;

#[cfg(feature = "rocksdb")]
const STORAGE_CACHE_SIZE_BYTES: usize = 64 * 1024 * 1024;
#[cfg(feature = "rocksdb")]
const CATALOGUE_ROCKSDB_DIR: &str = "catalogue.rocksdb";
#[cfg(feature = "sqlite")]
const CATALOGUE_SQLITE_FILE: &str = "catalogue.sqlite";
#[cfg(feature = "rocksdb")]
const CORE_SERVER_ROCKSDB_DIR: &str = "core-server.rocksdb";
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const EDGE_UPSTREAM_UNSUPPORTED_MESSAGE: &str = "edge upstream sync is temporarily unsupported while server-to-server sync is migrated to the core engine; refusing to start the legacy alpha transport";

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
    schema_mode: ServerSchemaMode,
    storage_backend: StorageBackend,
    core_server_schema: Option<JazzSchema>,
    sync_tracer: Option<crate::sync::SyncTracer>,
    upstream_url: Option<String>,
    shutdown_timeout: Duration,
    #[cfg(test)]
    allow_unsupported_upstream_for_catalogue_tests: bool,
}

impl ServerBuilder {
    pub fn new(app_id: AppId) -> Self {
        Self {
            app_id,
            auth_config: AuthConfig {
                allow_local_first_auth: true,
                ..Default::default()
            },
            schema_mode: ServerSchemaMode::Dynamic,
            storage_backend: StorageBackend::Persistent {
                path: PathBuf::from("./data"),
            },
            core_server_schema: None,
            sync_tracer: None,
            upstream_url: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            #[cfg(test)]
            allow_unsupported_upstream_for_catalogue_tests: false,
        }
    }

    pub fn with_sync_tracer(mut self, tracer: crate::sync::SyncTracer) -> Self {
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

    pub fn with_upstream_url(mut self, upstream_url: impl Into<String>) -> Self {
        self.upstream_url = Some(upstream_url.into());
        self
    }

    #[cfg(test)]
    pub(crate) fn allow_unsupported_upstream_for_catalogue_tests(mut self) -> Self {
        self.allow_unsupported_upstream_for_catalogue_tests = true;
        self
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
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

    pub fn with_core_server_schema(mut self, schema: JazzSchema) -> Self {
        self.core_server_schema = Some(schema);
        self
    }

    pub async fn build(self) -> Result<BuiltServer, String> {
        let auth_config = self.auth_config.clone();
        let topology = if self.upstream_url.is_some() {
            ServerTopology::Edge
        } else {
            ServerTopology::Core
        };
        let upstream_http_url = match self.upstream_url.as_deref() {
            Some(upstream_url) => Some(upstream_http_url(upstream_url, self.app_id)?),
            None => None,
        };
        validate_server_config(&auth_config, topology)?;
        #[cfg(test)]
        let allow_unsupported_upstream_for_catalogue_tests =
            self.allow_unsupported_upstream_for_catalogue_tests;
        #[cfg(not(test))]
        let allow_unsupported_upstream_for_catalogue_tests = false;
        validate_upstream_sync_supported(topology, allow_unsupported_upstream_for_catalogue_tests)?;
        let jwt_verifier = build_jwt_verifier(&auth_config).await?;
        log_auth_config(&auth_config, topology);

        let connection_event_hub = Arc::new(ConnectionEventHub::default());
        let (catalogue_store, latest_catalogue_schema) = self.build_catalogue_store()?;
        let http_client = reqwest::Client::builder()
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;

        let core_server_storage_config = self.build_core_server_storage_config();
        let core_server =
            self.build_core_server(latest_catalogue_schema, core_server_storage_config.clone())?;
        let core_server_storage_config = core_server_storage_config.ok();

        let state = Arc::new(ServerState {
            catalogue_store,
            catalogue: crate::server::ServerCatalogue,
            app_id: self.app_id,
            connections: RwLock::new(HashMap::new()),
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
            connection_event_hub,
            auth_config,
            upstream_http_url,
            topology,
            jwt_verifier,
            http_client,
            disconnect_candidates: RwLock::new(HashMap::new()),
            client_ttl: RwLock::new(Duration::from_secs(300)),
            sync_tracer: self.sync_tracer.clone(),
            core_server: std::sync::RwLock::new(core_server),
            core_server_storage_config,
            shutdown: crate::server::ShutdownController::new(self.shutdown_timeout),
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

    /// Build the direct admin catalogue store used by HTTP catalogue routes.
    ///
    fn build_catalogue_store(&self) -> Result<(DirectCatalogueStore, Option<Schema>), String> {
        let storage = self.build_catalogue_storage()?;
        let initial_schema = match &self.schema_mode {
            ServerSchemaMode::Fixed(schema) => Some(schema.clone()),
            ServerSchemaMode::Dynamic => None,
        };

        #[cfg(test)]
        let store = {
            let schema_branches = test_schema_branches(initial_schema.as_ref());
            let local_durability_tiers =
                std::collections::HashSet::from([self.local_durability_tier()]);
            DirectCatalogueStore::with_test_observability(
                self.app_id,
                initial_schema,
                storage,
                schema_branches,
                local_durability_tiers,
            )
        };
        #[cfg(not(test))]
        let store = DirectCatalogueStore::new(self.app_id, initial_schema, storage);

        let latest_catalogue_schema = store
            .latest_published_schema()
            .map_err(|error| format!("failed to read latest catalogue schema: {error:?}"))?;
        Ok((store, latest_catalogue_schema))
    }

    fn build_core_server(
        &self,
        latest_catalogue_schema: Option<Schema>,
        storage_config: Result<StorageConfig, String>,
    ) -> Result<Option<crate::server::core_server::CoreServer>, String> {
        if let Some(schema) = &self.core_server_schema {
            let storage_config = storage_config?;
            return Ok(Some(
                crate::server::core_server::CoreServer::start_with_storage(
                    schema.clone(),
                    storage_config,
                )?,
            ));
        }

        let schema = match &self.schema_mode {
            ServerSchemaMode::Fixed(schema) => Some(schema.clone()),
            ServerSchemaMode::Dynamic => latest_catalogue_schema,
        };
        let Some(schema) = schema else {
            return Ok(None);
        };
        let storage_config = storage_config?;
        let schema = crate::server::direct_schema::convert_alpha_schema(&schema)
            .map_err(|error| format!("failed to build core server schema: {error}"))?;
        Ok(Some(
            crate::server::core_server::CoreServer::start_with_storage(schema, storage_config)?,
        ))
    }

    fn build_core_server_storage_config(&self) -> Result<StorageConfig, String> {
        match &self.storage_backend {
            StorageBackend::InMemory => Ok(StorageConfig::InMemory),
            StorageBackend::Persistent { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;

                #[cfg(feature = "rocksdb")]
                {
                    Ok(StorageConfig::RocksDb {
                        path: path.join(CORE_SERVER_ROCKSDB_DIR),
                    })
                }
                #[cfg(not(feature = "rocksdb"))]
                {
                    Err("core server persistent storage requires the rocksdb feature".to_owned())
                }
            }
            #[cfg(feature = "rocksdb")]
            StorageBackend::RocksDb { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;
                Ok(StorageConfig::RocksDb {
                    path: path.join(CORE_SERVER_ROCKSDB_DIR),
                })
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite { .. } => {
                Err("core server storage does not support sqlite yet".to_owned())
            }
        }
    }

    fn build_catalogue_storage(&self) -> Result<DynStorage, String> {
        match &self.storage_backend {
            StorageBackend::Persistent { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;

                #[cfg(feature = "rocksdb")]
                {
                    let db_path = path.join(CATALOGUE_ROCKSDB_DIR);
                    let storage = RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES)
                        .map_err(|e| {
                            format!(
                                "failed to open catalogue storage '{}': {e:?}",
                                db_path.display()
                            )
                        })?;
                    Ok(Box::new(storage))
                }
                #[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
                {
                    let db_path = path.join(CATALOGUE_SQLITE_FILE);
                    let storage = SqliteStorage::open(&db_path).map_err(|e| {
                        format!(
                            "failed to open catalogue storage '{}': {e:?}",
                            db_path.display()
                        )
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
                let db_path = path.join(CATALOGUE_SQLITE_FILE);
                let storage = SqliteStorage::open(&db_path).map_err(|e| {
                    format!(
                        "failed to open catalogue storage '{}': {e:?}",
                        db_path.display()
                    )
                })?;
                Ok(Box::new(storage))
            }
            #[cfg(feature = "rocksdb")]
            StorageBackend::RocksDb { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;
                let db_path = path.join(CATALOGUE_ROCKSDB_DIR);
                let storage =
                    RocksDBStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES).map_err(|e| {
                        format!(
                            "failed to open catalogue storage '{}': {e:?}",
                            db_path.display()
                        )
                    })?;
                Ok(Box::new(storage))
            }
            StorageBackend::InMemory => Ok(Box::new(MemoryStorage::new())),
        }
    }

    #[cfg(test)]
    fn local_durability_tier(&self) -> DurabilityTier {
        if self.upstream_url.is_some() {
            DurabilityTier::EdgeServer
        } else {
            DurabilityTier::GlobalServer
        }
    }
}

#[cfg(test)]
fn test_schema_branches(schema: Option<&Schema>) -> Vec<String> {
    schema
        .map(|schema| {
            ComposedBranchName::new("prod", SchemaHash::compute(schema), "main")
                .to_branch_name()
                .as_str()
                .to_string()
        })
        .into_iter()
        .collect()
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
    topology: ServerTopology,
) -> Result<(), String> {
    if topology.is_edge() && auth_config.admin_secret.is_none() {
        return Err("edge mode requires --admin-secret / JAZZ_ADMIN_SECRET when --upstream-url / JAZZ_UPSTREAM_URL is set".to_string());
    }

    Ok(())
}

fn validate_upstream_sync_supported(
    topology: ServerTopology,
    allow_unsupported_upstream_for_catalogue_tests: bool,
) -> Result<(), String> {
    if topology.is_edge() && !allow_unsupported_upstream_for_catalogue_tests {
        return Err(EDGE_UPSTREAM_UNSUPPORTED_MESSAGE.to_owned());
    }

    Ok(())
}

fn log_auth_config(auth_config: &AuthConfig, topology: ServerTopology) {
    info!(
        "Auth configured: local_first={}, jwks={}, static_jwt_key={}, cookie={}, backend={}, admin={}, topology={:?}",
        auth_config.allow_local_first_auth,
        auth_config.jwks_url.is_some(),
        auth_config.jwt_public_key.is_some(),
        auth_config.auth_cookie_name.is_some(),
        auth_config.backend_secret.is_some(),
        auth_config.admin_secret.is_some(),
        topology
    );
}

pub fn upstream_http_url(base_url: &str, app_id: AppId) -> Result<String, String> {
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|err| format!("invalid upstream URL '{base_url}': {err}"))?;

    if url.query().is_some() || url.fragment().is_some() {
        return Err("upstream URL must not include query parameters or a fragment".to_string());
    }

    let scheme = match url.scheme() {
        "http" => "http",
        "https" => "https",
        "ws" => "http",
        "wss" => "https",
        other => {
            return Err(format!(
                "unsupported upstream URL scheme '{other}'; expected http, https, ws, or wss"
            ));
        }
    };
    url.set_scheme(scheme)
        .map_err(|_| format!("failed to set upstream URL scheme to {scheme}"))?;

    let app_ws_path = format!("/apps/{app_id}/ws");
    let normalized_path = url.path().trim_end_matches('/').to_string();
    if normalized_path == app_ws_path.trim_end_matches('/') {
        url.set_path("/");
    } else if let Some(prefix) = normalized_path.strip_suffix(&app_ws_path) {
        let prefix_path = if prefix.is_empty() {
            "/".to_string()
        } else {
            format!("{}/", prefix.trim_end_matches('/'))
        };
        url.set_path(&prefix_path);
    } else if normalized_path.is_empty() {
        url.set_path("/");
    }

    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_manager::AppId;

    #[tokio::test]
    async fn edge_upstream_mode_is_explicitly_unsupported() {
        let app_id =
            AppId::from_string("00000000-0000-0000-0000-000000000001").expect("parse app id");
        let auth_config = AuthConfig {
            admin_secret: Some("test-admin-secret".to_owned()),
            ..Default::default()
        };

        let result = ServerBuilder::new(app_id)
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("http://127.0.0.1:12345")
            .build()
            .await;

        assert_eq!(
            result.err().as_deref(),
            Some(EDGE_UPSTREAM_UNSUPPORTED_MESSAGE)
        );
    }

    #[test]
    fn upstream_http_url_conversion_maps_base_urls_to_app_routes() {
        let app_id =
            AppId::from_string("00000000-0000-0000-0000-000000000001").expect("parse app id");

        assert_eq!(
            upstream_http_url("https://core.example.com", app_id).expect("https conversion"),
            "https://core.example.com/"
        );
        assert_eq!(
            upstream_http_url("http://core.example.com/base/", app_id).expect("http conversion"),
            "http://core.example.com/base/"
        );
        assert_eq!(
            upstream_http_url("ws://core.example.com", app_id).expect("ws conversion"),
            "http://core.example.com/"
        );
        assert_eq!(
            upstream_http_url(
                "wss://core.example.com/apps/00000000-0000-0000-0000-000000000001/ws",
                app_id,
            )
            .expect("wss conversion"),
            "https://core.example.com/"
        );
        assert_eq!(
            upstream_http_url(
                "wss://core.example.com/base/apps/00000000-0000-0000-0000-000000000001/ws",
                app_id,
            )
            .expect("prefixed wss conversion"),
            "https://core.example.com/base/"
        );
    }

    #[test]
    fn upstream_http_url_conversion_rejects_query_and_fragment_urls() {
        let app_id =
            AppId::from_string("00000000-0000-0000-0000-000000000001").expect("parse app id");

        assert!(upstream_http_url("https://core.example.com?token=abc", app_id).is_err());
        assert!(upstream_http_url("https://core.example.com#cluster-a", app_id).is_err());
    }

    #[tokio::test]
    async fn builder_requires_admin_secret_in_edge_mode() {
        let auth_config = AuthConfig {
            allow_local_first_auth: true,
            ..Default::default()
        };

        let result = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await;
        let error = result
            .err()
            .expect("edge mode without admin secret should fail");

        assert!(error.contains("--admin-secret"));
        assert!(error.contains("--upstream-url"));
    }

    #[tokio::test]
    async fn builder_rejects_edge_mode_with_admin_secret_until_direct_core_upstream_exists() {
        let result = ServerBuilder::new(AppId::from_name("edge-builder-admin-secret-only"))
            .with_storage(StorageBackend::InMemory)
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_string()),
                ..Default::default()
            })
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await;

        assert_eq!(
            result.err().as_deref(),
            Some(EDGE_UPSTREAM_UNSUPPORTED_MESSAGE)
        );
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
            .catalogue_store
            .local_durability_tiers_for_test()
            .expect("read catalogue durability tiers");

        assert_eq!(
            tiers,
            std::collections::HashSet::from([DurabilityTier::GlobalServer])
        );
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn dynamic_builder_starts_core_server_from_rehydrated_catalogue_schema() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("dynamic-core-server-rehydrate");
        let schema = crate::schema_api::SchemaBuilder::new()
            .table(
                crate::schema_api::TableSchema::builder("todos")
                    .column("id", crate::schema_api::ColumnType::Uuid)
                    .column("title", crate::schema_api::ColumnType::Text),
            )
            .build();

        {
            let built = ServerBuilder::new(app_id)
                .with_schema(schema)
                .with_storage(StorageBackend::RocksDb {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build fixed schema server");
            assert!(built.state.core_server().is_some());
            built
                .state
                .catalogue_store
                .persist_schema()
                .expect("publish fixed schema catalogue");
            built
                .state
                .catalogue_store
                .flush()
                .expect("flush fixed schema catalogue");
        }

        let rebuilt = ServerBuilder::new(app_id)
            .with_storage(StorageBackend::RocksDb {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("build dynamic server from rehydrated catalogue");

        assert!(rebuilt.state.core_server().is_some());
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn rocksdb_builder_starts_core_server_with_catalogue_storage_after_restart() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-core-server-restart");
        let schema = crate::schema_api::SchemaBuilder::new()
            .table(
                crate::schema_api::TableSchema::builder("todos")
                    .column("id", crate::schema_api::ColumnType::Uuid)
                    .column("title", crate::schema_api::ColumnType::Text),
            )
            .build();

        {
            let built = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage(StorageBackend::RocksDb {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build RocksDB server with core server");

            assert!(built.state.core_server().is_some());
            assert!(data_dir.path().join(CATALOGUE_ROCKSDB_DIR).exists());
            assert!(data_dir.path().join(CORE_SERVER_ROCKSDB_DIR).exists());
        }

        let rebuilt = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_storage(StorageBackend::RocksDb {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("rebuild RocksDB server with core server");

        assert!(rebuilt.state.core_server().is_some());
        assert!(data_dir.path().join(CORE_SERVER_ROCKSDB_DIR).exists());
    }

    #[tokio::test]
    async fn builder_refuses_edge_tier_until_upstream_sync_uses_direct_core() {
        let result = ServerBuilder::new(AppId::from_name("edge-builder-tier"))
            .with_storage(StorageBackend::InMemory)
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_string()),
                ..Default::default()
            })
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await;

        assert_eq!(
            result.err().as_deref(),
            Some(EDGE_UPSTREAM_UNSUPPORTED_MESSAGE)
        );
    }
}

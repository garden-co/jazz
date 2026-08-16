use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use crate::db::WireTransportAdapter;
use crate::ids::AuthorId;
use crate::node::EdgeCacheBudget;
use crate::schema::JazzSchema;
use crate::serving::{NodeRole, StorageConfig};
use axum::Router;
use tracing::info;

use crate::tools::AppId;
use crate::tools::middleware::AuthConfig;
use crate::tools::middleware::auth::{
    JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache, JwtVerifier, StaticJwtVerifier,
};
use crate::tools::public_schema::Schema;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
use crate::tools::server::CatalogueRocksDbStorage;
use crate::tools::server::core_websocket_transport::WebSocketTransport;
use crate::tools::server::core_websocket_transport::validate_catalogue_bootstrap_upstream_url;
use crate::tools::server::routes;
use crate::tools::server::{
    CatalogueMemoryStorage, DynCatalogueStorage, ServerState, ServerTopology, StoredCatalogue,
};
#[cfg(test)]
use crate::tools::sync::DurabilityTier;

#[cfg(feature = "rocksdb")]
const STORAGE_CACHE_SIZE_BYTES: usize = 64 * 1024 * 1024;
#[cfg(feature = "rocksdb")]
const CATALOGUE_ROCKSDB_DIR: &str = "catalogue.rocksdb";
#[cfg(feature = "rocksdb")]
const SERVER_SHELL_ROCKSDB_DIR: &str = "server-shell.rocksdb";
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct BuiltServer {
    #[cfg_attr(not(test), allow(dead_code))]
    pub state: Arc<ServerState>,
    pub app: Router,
}

impl BuiltServer {
    /// Stop this server and wait until its owned runtime and durable storage
    /// have been closed.
    ///
    /// A builder owns a shell even when it is used without the test-server
    /// listener wrapper. Callers that reopen the same persistent path must use
    /// this lifecycle boundary rather than relying on field drop order. The
    /// close work runs on the server's dedicated lifecycle thread, so callers
    /// may await this method from any async executor. The operation is
    /// idempotent: subsequent calls return the terminal shutdown phase
    /// recorded by the state.
    pub async fn shutdown(&self) -> crate::tools::server::ShutdownPhase {
        self.state.shutdown.request_shutdown();
        self.state.run_shutdown_finalization().await
    }
}

#[cfg_attr(not(test), allow(dead_code))]
enum ServerSchemaMode {
    Dynamic,
    Fixed(Schema),
}

/// Storage backend selection for [`ServerBuilder::with_storage`].
///
/// `Persistent` requires the RocksDB feature for durable server shell
/// storage. SQLite remains a client/native storage backend, but is not a
/// supported server shell backend.
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
    core_server_shell_schema: Option<JazzSchema>,
    upstream_url: Option<String>,
    edge_cache_budget: Option<EdgeCacheBudget>,
    shutdown_timeout: Duration,
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
            core_server_shell_schema: None,
            upstream_url: None,
            edge_cache_budget: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
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

    pub fn with_edge_cache_budget(mut self, budget: EdgeCacheBudget) -> Self {
        self.edge_cache_budget = Some(budget);
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

    pub fn with_core_server_shell_schema(mut self, schema: JazzSchema) -> Self {
        self.core_server_shell_schema = Some(schema);
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
        if topology == ServerTopology::Edge {
            validate_catalogue_bootstrap_upstream_url(
                self.upstream_url
                    .as_deref()
                    .expect("edge topology has an upstream URL"),
                self.app_id,
            )?;
        }
        let jwt_verifier = build_jwt_verifier(&auth_config).await?;
        log_auth_config(&auth_config, topology);

        let (catalogue_store, latest_catalogue_schema) = self.build_catalogue_store()?;
        let http_client = reqwest::Client::builder()
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;

        let core_server_shell_storage_config = self.build_core_server_shell_storage_config();
        let core_server_shell = self.build_core_server_shell(
            latest_catalogue_schema,
            core_server_shell_storage_config.clone(),
            topology,
        )?;
        let dynamic_edge_catalogue_ready =
            topology != ServerTopology::Edge || core_server_shell.is_some();
        let core_server_shell_storage_config = core_server_shell_storage_config.ok();

        let state = Arc::new(ServerState {
            catalogue_store,
            catalogue: crate::tools::server::ServerCatalogue,
            app_id: self.app_id,
            auth_config,
            upstream_http_url,
            topology,
            jwt_verifier,
            http_client,
            core_server_shell: std::sync::RwLock::new(core_server_shell),
            core_server_shell_storage_config,
            // A validated durable catalogue remains usable while its core is
            // offline. Blank edges have no such generation and stay behind
            // RetryLater until authenticated bootstrap completes.
            dynamic_edge_catalogue_ready: AtomicBool::new(dynamic_edge_catalogue_ready),
            shutdown: crate::tools::server::ShutdownController::new(self.shutdown_timeout),
        });

        if let (ServerTopology::Edge, Some(upstream_url), Some(admin_secret)) = (
            topology,
            self.upstream_url.clone(),
            state.auth_config.admin_secret.clone(),
        ) {
            spawn_edge_upstream_connector(
                state.clone(),
                upstream_url,
                self.app_id,
                admin_secret,
                self.edge_cache_budget,
            );
        }

        let app = routes::create_router(state.clone());
        Ok(BuiltServer { state, app })
    }

    /// Build the direct admin catalogue store used by HTTP catalogue routes.
    ///
    fn build_catalogue_store(&self) -> Result<(StoredCatalogue, Option<Schema>), String> {
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
            StoredCatalogue::with_test_observability(
                self.app_id,
                initial_schema,
                storage,
                schema_branches,
                local_durability_tiers,
            )
        };
        #[cfg(not(test))]
        let store = StoredCatalogue::new(self.app_id, initial_schema, storage);

        let latest_catalogue_schema = store
            .latest_published_schema()
            .map_err(|error| format!("failed to read latest catalogue schema: {error:?}"))?;
        Ok((store, latest_catalogue_schema))
    }

    fn build_core_server_shell(
        &self,
        latest_catalogue_schema: Option<Schema>,
        storage_config: Result<StorageConfig, String>,
        topology: ServerTopology,
    ) -> Result<Option<crate::tools::server::core_server_shell::ServerShellHandle>, String> {
        let role = match topology {
            ServerTopology::Core => NodeRole::Core,
            ServerTopology::Edge => NodeRole::Edge,
        };
        if let Some(schema) = &self.core_server_shell_schema {
            let storage_config = storage_config?;
            return Ok(Some(
                crate::tools::server::core_server_shell::ServerShellHandle::start_with_storage_config(
                    schema.clone(),
                    storage_config,
                    role,
                    self.edge_cache_budget,
                )?,
            ));
        }

        let schema = match &self.schema_mode {
            ServerSchemaMode::Fixed(schema) => Some(schema.clone()),
            ServerSchemaMode::Dynamic => latest_catalogue_schema,
        };
        let Some(schema) = schema else {
            if topology == ServerTopology::Edge {
                return crate::tools::server::core_server_shell::ServerShellHandle::try_start_dynamic_edge_from_storage(
                    storage_config?,
                    self.edge_cache_budget,
                );
            }
            return Ok(None);
        };
        let storage_config = storage_config?;
        let schema = crate::tools::server::public_schema_convert::convert_public_schema(&schema)
            .map_err(|error| format!("failed to build server shell schema: {error}"))?;
        Ok(Some(
            crate::tools::server::core_server_shell::ServerShellHandle::start_with_storage_config(
                schema,
                storage_config,
                role,
                self.edge_cache_budget,
            )?,
        ))
    }

    fn build_core_server_shell_storage_config(&self) -> Result<StorageConfig, String> {
        match &self.storage_backend {
            StorageBackend::InMemory => Ok(StorageConfig::InMemory),
            StorageBackend::Persistent { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;

                #[cfg(feature = "rocksdb")]
                {
                    Ok(StorageConfig::RocksDb {
                        path: path.join(SERVER_SHELL_ROCKSDB_DIR),
                    })
                }
                #[cfg(not(feature = "rocksdb"))]
                {
                    Err("server shell persistent storage requires the rocksdb feature".to_owned())
                }
            }
            #[cfg(feature = "rocksdb")]
            StorageBackend::RocksDb { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;
                Ok(StorageConfig::RocksDb {
                    path: path.join(SERVER_SHELL_ROCKSDB_DIR),
                })
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite { .. } => {
                Err("server shell storage does not support sqlite yet".to_owned())
            }
        }
    }

    fn build_catalogue_storage(&self) -> Result<DynCatalogueStorage, String> {
        match &self.storage_backend {
            StorageBackend::Persistent { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;

                #[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
                {
                    let db_path = path.join(CATALOGUE_ROCKSDB_DIR);
                    let storage = CatalogueRocksDbStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES)
                        .map_err(|e| {
                            format!(
                                "failed to open catalogue storage '{}': {e:?}",
                                db_path.display()
                            )
                        })?;
                    Ok(Box::new(storage))
                }
                #[cfg(all(feature = "rocksdb", target_arch = "wasm32"))]
                {
                    Err("catalogue storage does not support rocksdb on wasm".to_owned())
                }
                #[cfg(not(all(feature = "rocksdb", not(target_arch = "wasm32"))))]
                {
                    Err("persistent catalogue storage requires the rocksdb feature".to_owned())
                }
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite { .. } => {
                Err("server catalogue storage does not support sqlite".to_owned())
            }
            #[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
            StorageBackend::RocksDb { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;
                let db_path = path.join(CATALOGUE_ROCKSDB_DIR);
                let storage = CatalogueRocksDbStorage::open(&db_path, STORAGE_CACHE_SIZE_BYTES)
                    .map_err(|e| {
                        format!(
                            "failed to open catalogue storage '{}': {e:?}",
                            db_path.display()
                        )
                    })?;
                Ok(Box::new(storage))
            }
            #[cfg(all(feature = "rocksdb", target_arch = "wasm32"))]
            StorageBackend::RocksDb { path } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| format!("failed to create data dir '{}': {e}", path.display()))?;
                Err("catalogue storage does not support rocksdb on wasm".to_owned())
            }
            StorageBackend::InMemory => Ok(Box::new(CatalogueMemoryStorage::new())),
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
    schema.map(|_| "main".to_string()).into_iter().collect()
}

fn spawn_edge_upstream_connector(
    state: Arc<ServerState>,
    upstream_url: String,
    app_id: AppId,
    admin_secret: String,
    edge_cache_budget: Option<EdgeCacheBudget>,
) {
    tokio::spawn(async move {
        let retry_delay = Duration::from_millis(100);
        loop {
            if state.shutdown.is_shutting_down() {
                return;
            }
            let auth = crate::tools::websocket_prelude_auth::AuthConfig {
                admin_secret: Some(admin_secret.clone()),
                ..Default::default()
            };
            // Revalidate every reconnect, including a durable reopen. The
            // regular sync socket can then carry application/fate traffic only
            // after the exact authority catalogue and local IVM registry are
            // complete for this process generation.
            let snapshot = match WebSocketTransport::connect_catalogue_bootstrap(
                &upstream_url,
                app_id,
                AuthorId::SYSTEM,
                auth.clone(),
            )
            .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    info!("edge catalogue bootstrap pending: {}", error);
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }
            };
            let shell = match state.core_server_shell() {
                Some(shell) => match state.refresh_dynamic_edge_catalogue(&shell, snapshot).await {
                    Ok(()) => shell,
                    Err(error) => {
                        info!("edge catalogue replay pending: {}", error);
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                },
                None => match state.start_dynamic_edge_shell(snapshot, edge_cache_budget) {
                    Ok(shell) => shell,
                    Err(error) => {
                        info!("edge catalogue bootstrap pending: {}", error);
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                },
            };
            let wake_shell = shell.clone();
            let wake = Arc::new(move || wake_shell.notify_activity());
            match WebSocketTransport::connect_with_wake(
                &upstream_url,
                app_id,
                AuthorId::SYSTEM,
                auth,
                wake,
            )
            .await
            {
                Ok(transport) => {
                    let (protocol_version, features, session_context) =
                        transport.negotiated_transport_metadata();
                    if shell
                        .connect_upstream(Box::new(WireTransportAdapter::new_with_session_context(
                            transport,
                            protocol_version,
                            features,
                            None,
                            session_context,
                        )))
                        .await
                        .is_ok()
                    {
                        state.mark_dynamic_edge_catalogue_ready();
                        shell.notify_activity();
                        return;
                    }
                }
                Err(error) => {
                    info!("edge upstream connection pending: {}", error);
                }
            }
            tokio::time::sleep(retry_delay).await;
        }
    });
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
    use crate::tools::AppId;
    use crate::tools::server::catalogue::CatalogueStore;

    async fn serve_for_dynamic_bootstrap(
        built: BuiltServer,
    ) -> (String, Arc<ServerState>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test listener address");
        let state = built.state.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, built.app)
                .await
                .expect("serve test server");
        });
        (format!("ws://{address}"), state, task)
    }

    fn dynamic_bootstrap_schema() -> crate::tools::public_schema::Schema {
        crate::tools::public_schema::SchemaBuilder::new()
            .table(
                crate::tools::public_schema::TableSchema::builder("notes")
                    .column("id", crate::tools::public_schema::ColumnType::Uuid)
                    .column("body", crate::tools::public_schema::ColumnType::Text),
            )
            .build()
    }

    #[tokio::test]
    async fn dynamic_edge_bootstraps_authenticated_catalogue_before_first_client() {
        let app_id = AppId::from_name("dynamic-edge-bootstrap-first-client");
        let auth = AuthConfig {
            admin_secret: Some("bootstrap-secret".to_owned()),
            ..Default::default()
        };
        let core = ServerBuilder::new(app_id)
            .with_schema(dynamic_bootstrap_schema())
            .with_auth_config(auth.clone())
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build authority core");
        let (core_url, core_state, core_task) = serve_for_dynamic_bootstrap(core).await;
        let expected = core_state
            .core_server_shell()
            .expect("core has runtime shell")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority catalogue");

        // This uses the separate snapshot-only wire exchange: no downstream
        // edge session or application schema exists before it succeeds.
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(auth.clone())
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url(core_url.clone())
            .build()
            .await
            .expect("build blank dynamic edge");
        assert!(edge.state.core_server_shell().is_none());
        let (edge_url, edge_state, edge_task) = serve_for_dynamic_bootstrap(edge).await;

        let ready_shell = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(shell) = edge_state.core_server_shell_for_client() {
                    return shell;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("edge becomes ready from idle bootstrap");
        assert_eq!(
            ready_shell
                .trusted_catalogue_snapshot_for_test()
                .await
                .expect("read adopted edge catalogue"),
            expected,
            "edge must adopt the authority genesis, lineage, and policy-bearing schema exactly"
        );

        // The first normal downstream connection is admitted only after Ready;
        // it is deliberately a separate connection from bootstrap.
        let client = WebSocketTransport::connect(
            &edge_url,
            app_id,
            AuthorId::from_bytes([0x44; 16]),
            crate::tools::websocket_prelude_auth::AuthConfig {
                admin_secret: Some("bootstrap-secret".to_owned()),
                ..Default::default()
            },
        )
        .await;
        assert!(
            client.is_ok(),
            "ready edge admits first normal client: {client:?}"
        );

        edge_task.abort();
        core_task.abort();
    }

    /// A dynamically bootstrapped Edge may have adopted a catalogue before its
    /// normal upstream session has been admitted. A downstream websocket in
    /// that interval must receive RetryLater rather than create a write whose
    /// final fate has no route back to its client session.
    #[tokio::test]
    async fn dynamic_edge_rejects_client_until_normal_upstream_session_is_attached() {
        let app_id = AppId::from_name("dynamic-edge-client-before-upstream");
        let auth = AuthConfig {
            admin_secret: Some("bootstrap-secret".to_owned()),
            ..Default::default()
        };
        let core = ServerBuilder::new(app_id)
            .with_schema(dynamic_bootstrap_schema())
            .with_auth_config(auth.clone())
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build authority core");
        let (_core_url, core_state, core_task) = serve_for_dynamic_bootstrap(core).await;
        let snapshot = core_state
            .core_server_shell()
            .expect("core has runtime shell")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority catalogue");

        // The unreachable upstream keeps the ordinary connector in its retry
        // loop. Publish only the authenticated snapshot as the narrow window
        // that used to admit a client before that connector attached.
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(auth.clone())
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("http://127.0.0.1:9")
            .build()
            .await
            .expect("build blank dynamic edge");
        edge.state
            .start_dynamic_edge_shell(snapshot, None)
            .expect("adopt dynamic edge catalogue");
        assert!(edge.state.core_server_shell().is_some());
        assert!(
            edge.state.core_server_shell_for_client().is_none(),
            "raw adopted shell is not externally Ready before normal upstream admission"
        );
        let (edge_url, _edge_state, edge_task) = serve_for_dynamic_bootstrap(edge).await;

        let error = WebSocketTransport::connect(
            &edge_url,
            app_id,
            AuthorId::from_bytes([0x44; 16]),
            crate::tools::websocket_prelude_auth::AuthConfig {
                admin_secret: Some("bootstrap-secret".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect_err("downstream admission waits for the normal upstream route");
        assert!(
            matches!(error, crate::tools::server::core_websocket_transport::WebSocketClientError::ServerRejected(ref message) if message.contains("bootstrapping") && message.contains("retry shortly")),
            "unready dynamic edge must give retryable admission failure: {error}"
        );

        edge_task.abort();
        core_task.abort();
    }

    #[tokio::test]
    async fn dynamic_catalogue_bootstrap_rejects_wrong_authority_credential() {
        let app_id = AppId::from_name("dynamic-edge-bootstrap-auth-denial");
        let core = ServerBuilder::new(app_id)
            .with_schema(dynamic_bootstrap_schema())
            .with_auth_config(AuthConfig {
                admin_secret: Some("right-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build authority core");
        let (core_url, _core_state, core_task) = serve_for_dynamic_bootstrap(core).await;
        let error = WebSocketTransport::connect_catalogue_bootstrap(
            core_url,
            app_id,
            AuthorId::SYSTEM,
            crate::tools::websocket_prelude_auth::AuthConfig {
                admin_secret: Some("wrong-secret".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect_err("wrong bootstrap credential must not obtain a catalogue");
        assert!(
            matches!(error, crate::tools::server::core_websocket_transport::WebSocketClientError::ServerRejected(ref message) if message.contains("AuthFailed")),
            "unexpected bootstrap auth result: {error}"
        );
        core_task.abort();
    }

    #[tokio::test]
    async fn dynamic_catalogue_bootstrap_requires_the_reserved_edge_identity() {
        let app_id = AppId::from_name("dynamic-edge-bootstrap-identity-denial");
        let core = ServerBuilder::new(app_id)
            .with_schema(dynamic_bootstrap_schema())
            .with_auth_config(AuthConfig {
                admin_secret: Some("bootstrap-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build authority core");
        let (core_url, _core_state, core_task) = serve_for_dynamic_bootstrap(core).await;
        let error = WebSocketTransport::connect_catalogue_bootstrap(
            core_url,
            app_id,
            AuthorId::from_bytes([0x46; 16]),
            crate::tools::websocket_prelude_auth::AuthConfig {
                admin_secret: Some("bootstrap-secret".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect_err("normal privileged client identity must not request bootstrap");
        assert!(
            matches!(error, crate::tools::server::core_websocket_transport::WebSocketClientError::ServerRejected(ref message) if message.contains("AuthFailed")),
            "bootstrap identity boundary returned {error}"
        );
        core_task.abort();
    }

    #[tokio::test]
    async fn dynamic_catalogue_bootstrap_rejects_generic_backend_credential() {
        let app_id = AppId::from_name("dynamic-edge-bootstrap-backend-denial");
        let core = ServerBuilder::new(app_id)
            .with_schema(dynamic_bootstrap_schema())
            .with_auth_config(AuthConfig {
                admin_secret: Some("bootstrap-admin-secret".to_owned()),
                backend_secret: Some("ordinary-backend-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build authority core");
        let (core_url, _core_state, core_task) = serve_for_dynamic_bootstrap(core).await;
        let error = WebSocketTransport::connect_catalogue_bootstrap(
            core_url,
            app_id,
            AuthorId::SYSTEM,
            crate::tools::websocket_prelude_auth::AuthConfig {
                backend_secret: Some("ordinary-backend-secret".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect_err("backend credential must not read an authority catalogue");
        assert!(
            matches!(error, crate::tools::server::core_websocket_transport::WebSocketClientError::ServerRejected(ref message) if message.contains("AuthFailed")),
            "generic backend credential crossed bootstrap boundary: {error}"
        );
        core_task.abort();
    }

    #[tokio::test]
    async fn dynamic_edge_keeps_unready_after_malformed_snapshot_then_accepts_retry() {
        let app_id = AppId::from_name("dynamic-edge-bootstrap-malformed-retry");
        let auth = AuthConfig {
            admin_secret: Some("bootstrap-secret".to_owned()),
            ..Default::default()
        };
        let core = ServerBuilder::new(app_id)
            .with_schema(dynamic_bootstrap_schema())
            .with_auth_config(auth.clone())
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build authority core");
        let snapshot = core
            .state
            .core_server_shell()
            .expect("core has shell")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority snapshot");
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(auth)
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await
            .expect("build blank edge");
        let mut malformed = snapshot.clone();
        malformed.schemas.push(
            malformed
                .schemas
                .first()
                .expect("authority has genesis")
                .clone(),
        );
        assert!(
            edge.state
                .start_dynamic_edge_shell(malformed, None)
                .is_err()
        );
        assert!(
            edge.state.core_server_shell().is_none(),
            "failed adoption must not publish a shell to downstream clients"
        );
        assert!(
            edge.state
                .start_dynamic_edge_shell(snapshot.clone(), None)
                .is_ok()
        );
        let first_shell = edge
            .state
            .core_server_shell()
            .expect("retry publishes ready shell");
        let second_shell = edge
            .state
            .start_dynamic_edge_shell(snapshot, None)
            .expect("duplicate driver wake is idempotent");
        assert_eq!(
            first_shell
                .trusted_catalogue_snapshot_for_test()
                .await
                .expect("read first ready shell"),
            second_shell
                .trusted_catalogue_snapshot_for_test()
                .await
                .expect("read duplicate-ready shell"),
            "a duplicate bootstrap wake reuses the already-published shell"
        );
    }

    #[tokio::test]
    async fn durable_dynamic_edge_gates_failed_catalogue_refresh_until_install_succeeds() {
        let app_id = AppId::from_name("dynamic-edge-refresh-install-failure");
        let auth = AuthConfig {
            admin_secret: Some("bootstrap-secret".to_owned()),
            ..Default::default()
        };
        let schema = dynamic_bootstrap_schema();
        let core = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_auth_config(auth.clone())
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build authority core");
        let snapshot = core
            .state
            .core_server_shell()
            .expect("core has shell")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority snapshot");
        let edge = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_auth_config(auth)
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await
            .expect("build edge with a validated durable generation");
        let shell = edge
            .state
            .core_server_shell()
            .expect("edge has ready shell");
        assert!(
            edge.state.core_server_shell_for_client().is_some(),
            "validated catalogue is usable while the upstream is offline"
        );

        let mut malformed = snapshot.clone();
        malformed.schemas.push(
            malformed
                .schemas
                .first()
                .expect("authority has genesis")
                .clone(),
        );
        assert!(
            edge.state
                .refresh_dynamic_edge_catalogue(&shell, malformed)
                .await
                .is_err()
        );
        assert!(
            edge.state.core_server_shell_for_client().is_none(),
            "failed validation/install must not advance the ready generation"
        );

        edge.state
            .refresh_dynamic_edge_catalogue(&shell, snapshot.clone())
            .await
            .expect("later complete refresh installs successfully");
        assert!(
            edge.state.core_server_shell_for_client().is_some(),
            "readiness advances only after the complete install returns"
        );

        let base = snapshot
            .schemas
            .first()
            .expect("authority has genesis")
            .clone();
        let evolved = crate::protocol::SchemaVersion::new(crate::schema::JazzSchema::new([
            crate::schema::TableSchema::new(
                "notes",
                [
                    groove::schema::ColumnSchema::new("id", groove::schema::ColumnType::Uuid),
                    groove::schema::ColumnSchema::new("body", groove::schema::ColumnType::String),
                    groove::schema::ColumnSchema::new("extra", groove::schema::ColumnType::String),
                ],
            ),
        ]));
        let mut evolved_snapshot = snapshot;
        evolved_snapshot.schemas.push(evolved.clone());
        evolved_snapshot.lineages.push((
            1,
            crate::protocol::SchemaLineagePublication::new(
                evolved.clone(),
                crate::protocol::MigrationLens::new(
                    base.id,
                    evolved.id,
                    vec![crate::protocol::TableLens {
                        source_table: "notes".to_owned(),
                        target_table: "notes".to_owned(),
                        ops: vec![crate::protocol::LensOp::AddColumn {
                            column: "extra".to_owned(),
                            default: groove::records::Value::String(String::new()),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            ),
        ));
        evolved_snapshot.current_write_schema = crate::protocol::CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        };
        shell
            .set_catalogue_activation_failpoint(
                crate::node::CatalogueActivationFailpoint::BeforeSnapshotActivationCommit,
            )
            .await
            .expect("arm post-registry install failure");
        assert!(
            edge.state
                .refresh_dynamic_edge_catalogue(&shell, evolved_snapshot)
                .await
                .is_err(),
            "v1-to-v2 activation fails after registry reconstruction at the planted boundary"
        );
        assert!(
            edge.state.core_server_shell_for_client().is_none(),
            "a post-registry activation failure must not publish the new ready generation"
        );
    }

    #[tokio::test]
    async fn blank_dynamic_edge_rejects_downstream_with_retry_later_until_ready() {
        let app_id = AppId::from_name("dynamic-edge-unready-downstream");
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(AuthConfig {
                admin_secret: Some("edge-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await
            .expect("build blank edge");
        assert!(edge.state.core_server_shell().is_none());
        let (edge_url, _edge_state, edge_task) = serve_for_dynamic_bootstrap(edge).await;
        let error = WebSocketTransport::connect(
            edge_url,
            app_id,
            AuthorId::from_bytes([0x45; 16]),
            crate::tools::websocket_prelude_auth::AuthConfig {
                admin_secret: Some("edge-secret".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect_err("unready edge must not admit a downstream session");
        assert!(
            matches!(error, crate::tools::server::core_websocket_transport::WebSocketClientError::ServerRejected(ref message) if message.contains("bootstrapping") && message.contains("retry shortly")),
            "unready edge must return an explicit retry-later diagnosis: {error}"
        );
        edge_task.abort();
    }

    #[tokio::test]
    async fn edge_upstream_mode_builds_with_admin_secret() {
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

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn edge_builder_rejects_remote_plaintext_bootstrap_upstream() {
        let result = ServerBuilder::new(AppId::from_name("edge-plaintext-bootstrap-rejected"))
            .with_storage(StorageBackend::InMemory)
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_owned()),
                ..Default::default()
            })
            .with_upstream_url("http://core.example.test")
            .build()
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("remote plaintext bootstrap must fail configuration"),
        };
        assert!(
            error.contains("plaintext ws:// bootstrap"),
            "error: {error}"
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
    async fn builder_accepts_edge_mode_with_admin_secret() {
        let built = ServerBuilder::new(AppId::from_name("edge-builder-admin-secret-only"))
            .with_storage(StorageBackend::InMemory)
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_string()),
                ..Default::default()
            })
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await
            .expect("build edge server with admin secret");

        assert!(built.state.topology.is_edge());
        assert!(built.state.upstream_http_url.is_some());
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
    async fn dynamic_builder_starts_core_server_shell_from_rehydrated_catalogue_schema() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("dynamic-server-shell-rehydrate");
        let schema = crate::tools::public_schema::SchemaBuilder::new()
            .table(
                crate::tools::public_schema::TableSchema::builder("todos")
                    .column("id", crate::tools::public_schema::ColumnType::Uuid)
                    .column("title", crate::tools::public_schema::ColumnType::Text),
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
            assert!(built.state.core_server_shell().is_some());
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

        assert!(rebuilt.state.core_server_shell().is_some());
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn rocksdb_builder_starts_core_server_shell_with_catalogue_storage_after_restart() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-restart");
        let schema = crate::tools::public_schema::SchemaBuilder::new()
            .table(
                crate::tools::public_schema::TableSchema::builder("todos")
                    .column("id", crate::tools::public_schema::ColumnType::Uuid)
                    .column("title", crate::tools::public_schema::ColumnType::Text),
            )
            .build();

        let retained_state = {
            let built = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage(StorageBackend::RocksDb {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build RocksDB server with server shell");

            assert!(built.state.core_server_shell().is_some());
            assert!(data_dir.path().join(CATALOGUE_ROCKSDB_DIR).exists());
            assert!(data_dir.path().join(SERVER_SHELL_ROCKSDB_DIR).exists());
            assert_eq!(
                built.shutdown().await,
                crate::tools::server::ShutdownPhase::StorageClosed,
                "the public builder lifecycle must join the shell before its RocksDB path is reopened"
            );
            Arc::clone(&built.state)
        };
        assert!(
            retained_state.core_server_shell().is_none(),
            "shutdown must retire the shell even if request/router state outlives BuiltServer"
        );

        let rebuilt = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_storage(StorageBackend::RocksDb {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("rebuild RocksDB server with server shell");

        assert!(rebuilt.state.core_server_shell().is_some());
        assert!(data_dir.path().join(SERVER_SHELL_ROCKSDB_DIR).exists());
        rebuilt.shutdown().await;

        // Some direct builder consumers own only `BuiltServer` and use Rust
        // scope exit as their lifecycle. Its last-shell fallback must join as
        // well: this reopen has no timeout or sleep to mask an owner-thread
        // race.
        {
            let dropped = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage(StorageBackend::RocksDb {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build RocksDB server for direct-drop lifecycle");
            assert!(dropped.state.core_server_shell().is_some());
        }

        let reopened_after_drop = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_storage(StorageBackend::RocksDb {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen RocksDB server after direct builder drop");
        assert!(reopened_after_drop.state.core_server_shell().is_some());
        assert_eq!(
            reopened_after_drop.shutdown().await,
            crate::tools::server::ShutdownPhase::StorageClosed
        );
        drop(retained_state);
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn rocksdb_builder_reopens_after_first_shutdown_waiter_is_aborted() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-aborted-shutdown");
        let schema = crate::tools::public_schema::SchemaBuilder::new()
            .table(
                crate::tools::public_schema::TableSchema::builder("todos")
                    .column("id", crate::tools::public_schema::ColumnType::Uuid),
            )
            .build();
        let built = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_storage(StorageBackend::RocksDb {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("build RocksDB server");
        let state = Arc::clone(&built.state);
        let request = state
            .shutdown
            .try_enter_app_request()
            .expect("running server accepts request");
        state.shutdown.request_shutdown();

        let first_state = Arc::clone(&state);
        let first = tokio::spawn(async move { first_state.run_shutdown_finalization().await });
        let mut phases = state.shutdown.subscribe();
        while *phases.borrow_and_update()
            != crate::tools::server::ShutdownPhase::DrainingConnections
        {
            phases
                .changed()
                .await
                .expect("detached finalizer remains alive");
        }
        first.abort();
        let _ = first.await;
        drop(request);
        assert_eq!(
            state.run_shutdown_finalization().await,
            crate::tools::server::ShutdownPhase::StorageClosed
        );

        let reopened = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_storage(StorageBackend::RocksDb {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen RocksDB after aborted shutdown waiter");
        assert_eq!(
            reopened.shutdown().await,
            crate::tools::server::ShutdownPhase::StorageClosed
        );
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_builder_shutdown_survives_initiating_runtime_drop() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-foreign-shutdown");
        let schema = crate::tools::public_schema::SchemaBuilder::new()
            .table(
                crate::tools::public_schema::TableSchema::builder("todos")
                    .column("id", crate::tools::public_schema::ColumnType::Uuid),
            )
            .build();
        let (built, state, request) = {
            let first_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("first shutdown runtime");
            let built = first_runtime
                .block_on(
                    ServerBuilder::new(app_id)
                        .with_schema(schema.clone())
                        .with_storage(StorageBackend::RocksDb {
                            path: data_dir.path().to_path_buf(),
                        })
                        .build(),
                )
                .expect("build RocksDB server");
            let state = Arc::clone(&built.state);
            let request = state
                .shutdown
                .try_enter_app_request()
                .expect("running server accepts request");
            state.shutdown.request_shutdown();
            first_runtime.block_on(async {
                let first_state = Arc::clone(&state);
                tokio::spawn(async move { first_state.run_shutdown_finalization().await });
                let mut phases = state.shutdown.subscribe();
                while *phases.borrow_and_update()
                    != crate::tools::server::ShutdownPhase::DrainingConnections
                {
                    phases
                        .changed()
                        .await
                        .expect("dedicated finalizer remains alive");
                }
            });
            // Dropping `first_runtime` cancels the initiating caller after it
            // has begun but before the request guard lets teardown progress.
            (built, state, request)
        };
        drop(request);

        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("second shutdown runtime");
        let reopened = second_runtime.block_on(async {
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(1), built.shutdown())
                    .await
                    .expect("later runtime reaches durable-close barrier"),
                crate::tools::server::ShutdownPhase::StorageClosed
            );
            assert!(state.core_server_shell().is_none());
            ServerBuilder::new(app_id)
                .with_schema(schema)
                .with_storage(StorageBackend::RocksDb {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("reopen RocksDB after live shutdown")
        });
        assert_eq!(
            second_runtime.block_on(reopened.shutdown()),
            crate::tools::server::ShutdownPhase::StorageClosed
        );
    }

    #[tokio::test]
    async fn builder_uses_edge_tier_with_upstream() {
        let built = ServerBuilder::new(AppId::from_name("edge-builder-tier"))
            .with_storage(StorageBackend::InMemory)
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_string()),
                ..Default::default()
            })
            .with_upstream_url("ws://127.0.0.1:9")
            .build()
            .await
            .expect("build edge server");

        let tiers = built
            .state
            .catalogue_store
            .local_durability_tiers_for_test()
            .expect("read catalogue durability tiers");

        assert_eq!(
            tiers,
            std::collections::HashSet::from([DurabilityTier::EdgeServer])
        );
    }
}

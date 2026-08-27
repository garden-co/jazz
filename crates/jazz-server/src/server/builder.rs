use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use axum::Router;
use jazz::groove::storage::StorageFactory;
use jazz::ids::AuthorSubject;
use jazz::node::EdgeCacheBudget;
use jazz::schema::JazzSchema;
use jazz::serving::{NodeRole, ServerUpstreamTerminalReason, StorageConfig};
use tracing::{error, info};

use crate::middleware::AuthConfig;
use crate::middleware::auth::{
    JWKS_CACHE_TTL, JWKS_MAX_STALE, JwksCache, JwtVerifier, StaticJwtVerifier,
};
use crate::server::routes;
use crate::server::{
    CatalogueKvStorage, CatalogueMemoryStorage, DynCatalogueStorage, EdgeUpstreamHealth,
    ServerState, ServerTopology, StoredCatalogue,
};
use jazz::tools::AppId;
use jazz::tools::native_transport_connector::{
    NativeTransportConnector, NativeTransportError, NativeTransportRequest, NativeTransportTerminal,
};
#[allow(deprecated)]
use jazz::tools::public_schema::Schema;
#[cfg(test)]
use jazz::tools::sync::DurabilityTier;

const CATALOGUE_ROCKSDB_DIR: &str = "catalogue.rocksdb";
const SERVER_SHELL_ROCKSDB_DIR: &str = "server-shell.rocksdb";
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const EDGE_RECONNECT_BASE_DELAY: Duration = Duration::from_millis(100);
const EDGE_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
const EDGE_RECONNECT_STABLE_AFTER: Duration = Duration::from_secs(30);

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
    pub async fn shutdown(&self) -> crate::server::ShutdownPhase {
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
/// `Persistent` requires a target-owned [`StorageFactory`] supplied at the
/// native composition boundary. SQLite remains a client/native storage
/// backend, but is not a supported server shell backend.
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
    native_transport_connector: Option<Arc<dyn NativeTransportConnector>>,
    storage_factory: Option<Arc<dyn StorageFactory>>,
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
            native_transport_connector: None,
            storage_factory: None,
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

    /// Supply the target-owned durable storage adapter.
    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(factory);
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

    /// Select the target-owned connector used by an edge's upstream link.
    pub fn with_native_transport_connector(
        mut self,
        connector: Arc<dyn NativeTransportConnector>,
    ) -> Self {
        self.native_transport_connector = Some(connector);
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
            if let Some(connector) = self.native_transport_connector.as_ref() {
                connector
                    .validate_catalogue_bootstrap_url(
                        self.upstream_url
                            .as_deref()
                            .expect("edge topology has an upstream URL"),
                        self.app_id,
                    )
                    .map_err(|error| error.to_string())?;
            }
            if self.native_transport_connector.is_none() {
                // Library unit tests exercise edge catalogue state without a
                // target-owned socket implementation. Native process shells
                // and all outward transport receipts supply their connector
                // at the composition boundary.
                #[cfg(not(test))]
                return Err("edge server requires a native transport connector".to_owned());
            }
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
            catalogue: crate::server::ServerCatalogue,
            app_id: self.app_id,
            auth_config,
            upstream_http_url,
            topology,
            jwt_verifier,
            http_client,
            core_server_shell: std::sync::RwLock::new(core_server_shell),
            core_server_shell_storage_config,
            storage_factory: self.storage_factory.clone(),
            runtime_catalogue_publication: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            runtime_catalogue_before_publication_hook: std::sync::Mutex::new(None),
            #[cfg(test)]
            runtime_catalogue_after_permissions_read_hook: std::sync::Mutex::new(None),
            // A validated durable catalogue remains usable while its core is
            // offline. Blank edges have no such generation and stay behind
            // RetryLater until authenticated bootstrap completes.
            dynamic_edge_catalogue_ready: AtomicBool::new(dynamic_edge_catalogue_ready),
            edge_upstream_health: std::sync::RwLock::new(EdgeUpstreamHealth::NotConfigured),
            edge_upstream_task: std::sync::Mutex::new(None),
            shutdown: crate::server::ShutdownController::new(self.shutdown_timeout),
        });

        if let (ServerTopology::Edge, Some(upstream_url), Some(admin_secret), Some(connector)) = (
            topology,
            self.upstream_url.clone(),
            state.auth_config.admin_secret.clone(),
            self.native_transport_connector,
        ) {
            spawn_edge_upstream_connector(
                state.clone(),
                upstream_url,
                self.app_id,
                admin_secret,
                self.edge_cache_budget,
                connector,
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
            .map_err(|error| format!("failed to read durable catalogue: {error}"))?
        };
        #[cfg(not(test))]
        let store = StoredCatalogue::new(self.app_id, initial_schema, storage)
            .map_err(|error| format!("failed to read durable catalogue: {error}"))?;

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
    ) -> Result<Option<crate::server::ServerRuntimeHandle>, String> {
        let role = match topology {
            ServerTopology::Core => NodeRole::Core,
            ServerTopology::Edge => NodeRole::Edge,
        };
        if let Some(schema) = &self.core_server_shell_schema {
            let storage_config = storage_config?;
            return Ok(Some(
                crate::server::ServerRuntimeHandle::start_with_storage_config(
                    schema.clone(),
                    storage_config,
                    self.storage_factory.clone(),
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
                return crate::server::ServerRuntimeHandle::try_start_dynamic_edge_from_storage(
                    storage_config?,
                    self.storage_factory.clone(),
                    self.edge_cache_budget,
                );
            }
            return Ok(None);
        };
        let storage_config = storage_config?;
        let schema = jazz::schema::JazzSchema::new(&schema)
            .map_err(|error| format!("failed to build server shell schema: {error}"))?;
        Ok(Some(
            crate::server::ServerRuntimeHandle::start_with_storage_config(
                schema,
                storage_config,
                self.storage_factory.clone(),
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

                let factory = self.storage_factory.as_ref().ok_or_else(|| {
                    "persistent catalogue storage requires a target-shell storage factory"
                        .to_owned()
                })?;
                let db_path = path.join(CATALOGUE_ROCKSDB_DIR);
                let storage = CatalogueKvStorage::open(Arc::clone(factory), db_path.clone())
                    .map_err(|error| {
                        format!(
                            "failed to open catalogue storage '{}': {error}",
                            db_path.display()
                        )
                    })?;
                Ok(Box::new(storage))
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite { .. } => {
                Err("server catalogue storage does not support sqlite".to_owned())
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

#[derive(Debug)]
enum EdgeConnectorOutcome {
    Retryable(String),
    Reconnect(String),
    Fatal(String),
    Stopped,
}

fn native_transport_outcome(error: NativeTransportError) -> EdgeConnectorOutcome {
    EdgeConnectorOutcome::Retryable(error.to_string())
}

fn connected_transport_outcome(reason: ServerUpstreamTerminalReason) -> EdgeConnectorOutcome {
    match reason {
        ServerUpstreamTerminalReason::NativeTransport(NativeTransportTerminal::PeerClosed(
            reason,
        )) => EdgeConnectorOutcome::Reconnect(reason),
        ServerUpstreamTerminalReason::NativeTransport(NativeTransportTerminal::OwnerDropped) => {
            EdgeConnectorOutcome::Stopped
        }
        ServerUpstreamTerminalReason::NativeTransport(NativeTransportTerminal::Failed(error)) => {
            EdgeConnectorOutcome::Reconnect(error.to_string())
        }
        ServerUpstreamTerminalReason::TransportFailed(reason) => {
            EdgeConnectorOutcome::Reconnect(reason)
        }
        ServerUpstreamTerminalReason::ProtocolFailed(reason) => EdgeConnectorOutcome::Fatal(reason),
        // A local owner cancellation is shutdown/control flow, not a remote
        // close that should create another connection generation.
        ServerUpstreamTerminalReason::Cancelled => EdgeConnectorOutcome::Stopped,
        ServerUpstreamTerminalReason::RuntimeStopped => {
            EdgeConnectorOutcome::Fatal("server shell upstream driver stopped".to_owned())
        }
    }
}

fn edge_reconnect_delay(attempt: u32) -> Duration {
    let multiplier = 1_u32 << attempt.saturating_sub(1).min(6);
    EDGE_RECONNECT_BASE_DELAY
        .checked_mul(multiplier)
        .unwrap_or(EDGE_RECONNECT_MAX_DELAY)
        .min(EDGE_RECONNECT_MAX_DELAY)
}

fn spawn_edge_upstream_connector(
    state: Arc<ServerState>,
    upstream_url: String,
    app_id: AppId,
    admin_secret: String,
    edge_cache_budget: Option<EdgeCacheBudget>,
    connector: Arc<dyn NativeTransportConnector>,
) {
    state.set_edge_upstream_health(EdgeUpstreamHealth::Connecting);
    let weak_state = Arc::downgrade(&state);
    let shutdown = state.shutdown.clone();
    let task = tokio::spawn(async move {
        let mut recovery_attempts = 0_u32;
        loop {
            let auth = jazz::tools::websocket_prelude_auth::AuthConfig {
                admin_secret: Some(admin_secret.clone()),
                ..Default::default()
            };
            let bootstrap = connector.bootstrap_catalogue(NativeTransportRequest {
                server_url: upstream_url.clone(),
                app_id,
                peer_identity: AuthorSubject::SYSTEM,
                auth: auth.clone(),
                wake: Arc::new(|| {}),
            });
            let snapshot = tokio::select! {
                biased;
                _ = shutdown.wait_requested() => return,
                result = bootstrap => match result {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        let outcome = native_transport_outcome(error);
                        if !handle_edge_connector_outcome(
                            &weak_state,
                            &shutdown,
                            outcome,
                            &mut recovery_attempts,
                        ).await {
                            return;
                        }
                        continue;
                    }
                },
            };

            let Some(state) = weak_state.upgrade() else {
                return;
            };
            let shell = match state.runtime() {
                Some(shell) => {
                    let refresh = state.refresh_dynamic_edge_catalogue(&shell, snapshot);
                    let refreshed = tokio::select! {
                        biased;
                        _ = shutdown.wait_requested() => return,
                        result = refresh => result,
                    };
                    match refreshed {
                        Ok(()) => shell,
                        Err(error) => {
                            drop(state);
                            if !handle_edge_connector_outcome(
                                &weak_state,
                                &shutdown,
                                EdgeConnectorOutcome::Fatal(format!(
                                    "edge catalogue refresh failed: {error}"
                                )),
                                &mut recovery_attempts,
                            )
                            .await
                            {
                                return;
                            }
                            continue;
                        }
                    }
                }
                None => match state.start_dynamic_edge_shell(snapshot, edge_cache_budget) {
                    Ok(shell) => shell,
                    Err(_) if shutdown.is_shutting_down() => return,
                    Err(error) => {
                        drop(state);
                        if !handle_edge_connector_outcome(
                            &weak_state,
                            &shutdown,
                            EdgeConnectorOutcome::Fatal(format!(
                                "edge catalogue bootstrap failed: {error}"
                            )),
                            &mut recovery_attempts,
                        )
                        .await
                        {
                            return;
                        }
                        continue;
                    }
                },
            };
            drop(state);

            let wake_shell = shell.clone();
            let wake = Arc::new(move || wake_shell.notify_activity());
            let connect = connector.connect(NativeTransportRequest {
                server_url: upstream_url.clone(),
                app_id,
                peer_identity: AuthorSubject::SYSTEM,
                auth,
                wake,
            });
            let connected = tokio::select! {
                biased;
                _ = shutdown.wait_requested() => return,
                result = connect => match result {
                    Ok(connected) => connected,
                    Err(error) => {
                        let outcome = native_transport_outcome(error);
                        if !handle_edge_connector_outcome(
                            &weak_state,
                            &shutdown,
                            outcome,
                            &mut recovery_attempts,
                        ).await {
                            return;
                        }
                        continue;
                    }
                },
            };
            let connection = tokio::select! {
                biased;
                _ = shutdown.wait_requested() => return,
                result = shell.connect_upstream_wire(
                    connected.transport,
                    connected.terminal,
                    connected.protocol_version,
                    connected.features,
                    connected.session_context,
                ) => match result {
                    Ok(connection) => connection,
                    Err(error) => {
                        if !handle_edge_connector_outcome(
                            &weak_state,
                            &shutdown,
                            EdgeConnectorOutcome::Fatal(format!(
                                "edge upstream attachment failed: {error}"
                            )),
                            &mut recovery_attempts,
                        ).await {
                            return;
                        }
                        continue;
                    }
                },
            };

            let Some(state) = weak_state.upgrade() else {
                return;
            };
            if let Err(error) = state.mark_dynamic_edge_catalogue_ready() {
                if shutdown.is_shutting_down() {
                    return;
                }
                state.set_edge_upstream_health(EdgeUpstreamHealth::Failed {
                    reason: error.clone(),
                });
                error!(%error, "edge upstream lifecycle failed");
                return;
            }
            state.set_edge_upstream_health(EdgeUpstreamHealth::Connected);
            shell.notify_activity();
            drop(state);

            let connected_at = Instant::now();
            let terminal = tokio::select! {
                biased;
                _ = shutdown.wait_requested() => return,
                terminal = connection.terminal() => terminal,
            };
            if connected_at.elapsed() >= EDGE_RECONNECT_STABLE_AFTER {
                recovery_attempts = 0;
            }
            let outcome = connected_transport_outcome(terminal);
            if !handle_edge_connector_outcome(
                &weak_state,
                &shutdown,
                outcome,
                &mut recovery_attempts,
            )
            .await
            {
                return;
            }
        }
    });
    state.own_edge_upstream_task(task);
}

async fn handle_edge_connector_outcome(
    state: &std::sync::Weak<ServerState>,
    shutdown: &crate::server::ShutdownController,
    outcome: EdgeConnectorOutcome,
    recovery_attempts: &mut u32,
) -> bool {
    let (reason, reconnect) = match outcome {
        EdgeConnectorOutcome::Stopped => {
            if let Some(state) = state.upgrade() {
                state.set_edge_upstream_health(EdgeUpstreamHealth::Stopped);
            }
            return false;
        }
        EdgeConnectorOutcome::Fatal(reason) => {
            if let Some(state) = state.upgrade() {
                state.set_edge_upstream_health(EdgeUpstreamHealth::Failed {
                    reason: reason.clone(),
                });
            }
            error!(%reason, "edge upstream lifecycle stopped");
            return false;
        }
        EdgeConnectorOutcome::Retryable(reason) => (reason, false),
        EdgeConnectorOutcome::Reconnect(reason) => (reason, true),
    };
    *recovery_attempts = recovery_attempts.saturating_add(1);
    let delay = edge_reconnect_delay(*recovery_attempts);
    if let Some(state) = state.upgrade() {
        state.set_edge_upstream_health(EdgeUpstreamHealth::Reconnecting {
            reason: reason.clone(),
        });
    } else {
        return false;
    }
    if reconnect {
        info!(%reason, ?delay, "edge upstream disconnected; reconnecting");
    } else {
        info!(%reason, ?delay, "edge upstream unavailable; retrying");
    }
    tokio::select! {
        biased;
        _ = shutdown.wait_requested() => false,
        _ = tokio::time::sleep(delay) => true,
    }
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
    use crate::server::catalogue::CatalogueStore;
    use crate::server::catalogue_entry::CatalogueEntry;
    use jazz::groove::storage::OrderedKvStorage;
    use jazz::tools::AppId;
    use jazz::tools::metadata::{MetadataKey, ObjectType};
    use jazz::tools::native_transport_connector::{
        ConnectedNativeTransport, NativeCatalogueBootstrapFuture, NativeTransportFuture,
    };
    use jazz::tools::public_schema::SchemaHash;
    use jazz::tools::schema_lens::LensTransform;
    use jazz::wire::{TransportError, WireTransport};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dynamic_bootstrap_schema() -> jazz::tools::public_schema::Schema {
        jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("notes")
                    .column("id", jazz::tools::public_schema::ColumnType::Uuid)
                    .column("body", jazz::tools::public_schema::ColumnType::Text),
            )
            .build()
    }

    fn write_raw_catalogue_entry(catalogue_path: &std::path::Path, entry: &CatalogueEntry) {
        let storage = jazz_storage_rocksdb::RocksDbStorage::open(catalogue_path, &["default"])
            .expect("open raw catalogue storage");
        jazz::db::block_on(storage.set(
            "default".to_owned(),
            format!("cat:{}", entry.object_id.uuid().simple()).into_bytes(),
            entry.encode_storage_row().expect("encode catalogue entry"),
        ))
        .expect("write raw catalogue entry");
    }

    struct NoopWireTransport;

    impl WireTransport for NoopWireTransport {
        fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }

    struct ClosingConnector {
        snapshot: jazz::protocol::CatalogueSnapshot,
        connect_count: AtomicUsize,
    }

    impl NativeTransportConnector for ClosingConnector {
        fn connect(&self, _request: NativeTransportRequest) -> NativeTransportFuture {
            let connection = self.connect_count.fetch_add(1, Ordering::SeqCst);
            let terminal = if connection == 0 {
                Box::pin(std::future::ready(NativeTransportTerminal::PeerClosed(
                    "idle websocket closed".to_owned(),
                )))
                    as jazz::tools::native_transport_connector::NativeTransportTerminalFuture
            } else {
                Box::pin(std::future::pending())
            };
            Box::pin(async move {
                Ok(ConnectedNativeTransport {
                    transport: Box::new(NoopWireTransport),
                    protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                    features: jazz::wire::FEATURE_NONE,
                    session_context: None,
                    terminal,
                })
            })
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> NativeCatalogueBootstrapFuture {
            let snapshot = self.snapshot.clone();
            Box::pin(async move { Ok(snapshot) })
        }
    }

    struct OwnerDroppingConnector {
        snapshot: jazz::protocol::CatalogueSnapshot,
        connect_count: AtomicUsize,
    }

    impl NativeTransportConnector for OwnerDroppingConnector {
        fn connect(&self, _request: NativeTransportRequest) -> NativeTransportFuture {
            self.connect_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(ConnectedNativeTransport {
                    transport: Box::new(NoopWireTransport),
                    protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                    features: jazz::wire::FEATURE_NONE,
                    session_context: None,
                    terminal: Box::pin(std::future::ready(NativeTransportTerminal::OwnerDropped)),
                })
            })
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> NativeCatalogueBootstrapFuture {
            let snapshot = self.snapshot.clone();
            Box::pin(async move { Ok(snapshot) })
        }
    }

    struct PendingTerminalConnector {
        snapshot: jazz::protocol::CatalogueSnapshot,
        connect_count: AtomicUsize,
    }

    impl NativeTransportConnector for PendingTerminalConnector {
        fn connect(&self, _request: NativeTransportRequest) -> NativeTransportFuture {
            self.connect_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(ConnectedNativeTransport {
                    transport: Box::new(NoopWireTransport),
                    protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                    features: jazz::wire::FEATURE_NONE,
                    session_context: None,
                    terminal: Box::pin(std::future::pending()),
                })
            })
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> NativeCatalogueBootstrapFuture {
            let snapshot = self.snapshot.clone();
            Box::pin(async move { Ok(snapshot) })
        }
    }

    struct MalformedWireTransport {
        returned_frame: bool,
    }

    impl WireTransport for MalformedWireTransport {
        fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            if self.returned_frame {
                None
            } else {
                self.returned_frame = true;
                Some(vec![0xff])
            }
        }
    }

    struct FatalProtocolConnector {
        snapshot: jazz::protocol::CatalogueSnapshot,
    }

    impl NativeTransportConnector for FatalProtocolConnector {
        fn connect(&self, _request: NativeTransportRequest) -> NativeTransportFuture {
            Box::pin(async {
                Ok(ConnectedNativeTransport {
                    transport: Box::new(MalformedWireTransport {
                        returned_frame: false,
                    }),
                    protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                    features: jazz::wire::FEATURE_NONE,
                    session_context: None,
                    terminal: Box::pin(std::future::pending()),
                })
            })
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> NativeCatalogueBootstrapFuture {
            let snapshot = self.snapshot.clone();
            Box::pin(async move { Ok(snapshot) })
        }
    }

    struct PendingBootstrapConnector;

    impl NativeTransportConnector for PendingBootstrapConnector {
        fn connect(&self, _request: NativeTransportRequest) -> NativeTransportFuture {
            Box::pin(std::future::pending())
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> NativeCatalogueBootstrapFuture {
            Box::pin(std::future::pending())
        }
    }

    #[test]
    fn edge_reconnect_delay_is_exponential_and_capped() {
        assert_eq!(edge_reconnect_delay(1), Duration::from_millis(100));
        assert_eq!(edge_reconnect_delay(2), Duration::from_millis(200));
        assert_eq!(edge_reconnect_delay(6), Duration::from_millis(3_200));
        assert_eq!(edge_reconnect_delay(7), EDGE_RECONNECT_MAX_DELAY);
        assert_eq!(edge_reconnect_delay(u32::MAX), EDGE_RECONNECT_MAX_DELAY);
    }

    #[tokio::test]
    async fn idle_upstream_terminal_reconnects_and_reaches_connected_again() {
        let app_id = AppId::from_name("edge-idle-upstream-reconnect");
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
            .runtime()
            .expect("core has shell")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority snapshot");
        let connector = Arc::new(ClosingConnector {
            snapshot,
            connect_count: AtomicUsize::new(0),
        });
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(auth)
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .with_native_transport_connector(connector.clone())
            .build()
            .await
            .expect("build edge");

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if connector.connect_count.load(Ordering::SeqCst) >= 2
                    && edge.state.edge_upstream_health() == EdgeUpstreamHealth::Connected
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("edge reconnects after idle terminal");

        assert!(connector.connect_count.load(Ordering::SeqCst) >= 2);
        edge.shutdown().await;
        core.shutdown().await;
    }

    #[tokio::test]
    async fn owner_drop_stops_connected_edge_upstream_without_reconnect() {
        let app_id = AppId::from_name("edge-owner-drop-stopped-health");
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
            .runtime()
            .expect("core has shell")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority snapshot");
        let connector = Arc::new(OwnerDroppingConnector {
            snapshot,
            connect_count: AtomicUsize::new(0),
        });
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(auth)
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .with_native_transport_connector(connector.clone())
            .build()
            .await
            .expect("build edge");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if edge.state.edge_upstream_health() == EdgeUpstreamHealth::Stopped {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner drop publishes stopped health");
        assert_eq!(
            connector.connect_count.load(Ordering::SeqCst),
            1,
            "owner drop must stop the connector rather than opening a replacement transport"
        );

        edge.shutdown().await;
        core.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_cancels_connected_edge_upstream_without_reconnect() {
        let app_id = AppId::from_name("edge-connected-cancellation-stopped-health");
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
            .runtime()
            .expect("core has shell")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority snapshot");
        let connector = Arc::new(PendingTerminalConnector {
            snapshot,
            connect_count: AtomicUsize::new(0),
        });
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(auth)
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .with_native_transport_connector(connector.clone())
            .build()
            .await
            .expect("build edge");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if edge.state.edge_upstream_health() == EdgeUpstreamHealth::Connected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("edge reaches connected before cancellation");
        edge.shutdown().await;
        assert_eq!(
            edge.state.edge_upstream_health(),
            EdgeUpstreamHealth::Stopped,
            "cancelling the attached driver publishes stopped health"
        );
        assert_eq!(
            connector.connect_count.load(Ordering::SeqCst),
            1,
            "cancellation must not open a replacement transport"
        );
        core.shutdown().await;
    }

    #[tokio::test]
    async fn fatal_upstream_protocol_error_stops_connector_with_visible_health_failure() {
        let app_id = AppId::from_name("edge-fatal-upstream-health");
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
            .runtime()
            .expect("core has shell")
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read authority snapshot");
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(auth)
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .with_native_transport_connector(Arc::new(FatalProtocolConnector { snapshot }))
            .build()
            .await
            .expect("build edge");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    edge.state.edge_upstream_health(),
                    EdgeUpstreamHealth::Failed { .. }
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fatal protocol outcome becomes visible");
        let health = edge.state.edge_upstream_health();
        assert!(
            matches!(
                &health,
                EdgeUpstreamHealth::Failed { reason }
                    if reason.contains("malformed auxiliary wire frame")
            ),
            "fatal protocol reason remains available to health reporting"
        );

        edge.shutdown().await;
        core.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_cancels_pending_edge_bootstrap_before_shell_publication() {
        let app_id = AppId::from_name("edge-shutdown-cancels-bootstrap");
        let edge = ServerBuilder::new(app_id)
            .with_auth_config(AuthConfig {
                admin_secret: Some("bootstrap-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .with_upstream_url("ws://127.0.0.1:9")
            .with_native_transport_connector(Arc::new(PendingBootstrapConnector))
            .build()
            .await
            .expect("build edge");

        let phase = tokio::time::timeout(Duration::from_secs(1), edge.shutdown())
            .await
            .expect("shutdown cancels pending bootstrap");
        assert_eq!(phase, crate::server::ShutdownPhase::StorageClosed);
        assert!(edge.state.runtime().is_none());
        assert_eq!(
            edge.state.edge_upstream_health(),
            EdgeUpstreamHealth::Stopped
        );
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
            .runtime()
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
            edge.state.runtime().is_none(),
            "failed adoption must not publish a shell to downstream clients"
        );
        assert!(
            edge.state
                .start_dynamic_edge_shell(snapshot.clone(), None)
                .is_ok()
        );
        let first_shell = edge.state.runtime().expect("retry publishes ready shell");
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
            .runtime()
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
        let shell = edge.state.runtime().expect("edge has ready shell");
        assert!(
            edge.state.runtime_for_client().is_some(),
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
            edge.state.runtime_for_client().is_none(),
            "failed validation/install must not advance the ready generation"
        );

        edge.state
            .refresh_dynamic_edge_catalogue(&shell, snapshot.clone())
            .await
            .expect("later complete refresh installs successfully");
        assert!(
            edge.state.runtime_for_client().is_some(),
            "readiness advances only after the complete install returns"
        );

        let base = snapshot
            .schemas
            .first()
            .expect("authority has genesis")
            .clone();
        let evolved_source = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("notes")
                    .column("id", jazz::tools::public_schema::ColumnType::Uuid)
                    .column("body", jazz::tools::public_schema::ColumnType::Text)
                    .column("extra", jazz::tools::public_schema::ColumnType::Text),
            )
            .build();
        let evolved_runtime =
            jazz::schema::JazzSchema::new(&evolved_source).expect("evolved public schema compiles");
        let evolved = jazz::protocol::SchemaVersion::new(evolved_runtime);
        let mut evolved_snapshot = snapshot;
        evolved_snapshot.schemas.push(evolved.clone());
        evolved_snapshot.lineages.push((
            1,
            jazz::protocol::SchemaLineagePublication::new(
                evolved.clone(),
                jazz::protocol::MigrationLens::new(
                    base.id,
                    evolved.id,
                    vec![jazz::protocol::TableLens {
                        source_table: "notes".to_owned(),
                        target_table: "notes".to_owned(),
                        ops: vec![jazz::protocol::LensOp::AddColumn {
                            column: "extra".to_owned(),
                            default: groove::records::Value::String(String::new()),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            ),
        ));
        evolved_snapshot.current_write_schema = jazz::protocol::CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        };
        shell
            .set_catalogue_activation_failpoint(
                jazz::node::CatalogueActivationFailpoint::BeforeSnapshotActivationCommit,
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
            edge.state.runtime_for_client().is_none(),
            "a post-registry activation failure must not publish the new ready generation"
        );
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

    #[tokio::test]
    async fn persistent_builder_fails_when_catalogue_scan_is_corrupt() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
        {
            let storage = jazz_storage_rocksdb::RocksDbStorage::open(&catalogue_path, &["default"])
                .expect("open raw catalogue storage");
            jazz::db::block_on(storage.set(
                "default".to_owned(),
                b"cat:not-a-uuid".to_vec(),
                vec![0],
            ))
            .expect("write malformed catalogue entry");
        }

        let result = ServerBuilder::new(AppId::from_name("corrupt-durable-catalogue"))
            .with_schema(dynamic_bootstrap_schema())
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await;

        let error = result.err().expect(
            "server startup must fail rather than treating a corrupt durable catalogue as empty",
        );
        assert!(
            error.contains(
                "failed to read durable catalogue: Storage error: IO error: catalogue key uuid"
            ),
            "startup error retains the catalogue-read context and storage corruption: {error}"
        );

        let storage = jazz_storage_rocksdb::RocksDbStorage::open(&catalogue_path, &["default"])
            .expect("failed startup releases the catalogue RocksDB lock");
        jazz::db::block_on(storage.delete("default".to_owned(), b"cat:not-a-uuid".to_vec()))
            .expect("remove corrupt catalogue entry");
        drop(storage);
        ServerBuilder::new(AppId::from_name("corrupt-durable-catalogue"))
            .with_schema(dynamic_bootstrap_schema())
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("builder retries after repaired catalogue");
    }

    #[tokio::test]
    async fn persistent_builder_fails_when_known_catalogue_payload_is_corrupt() {
        for (object_type, decode_context) in [
            (ObjectType::CatalogueSchema, "decode schema payload"),
            (ObjectType::CatalogueLens, "decode lens payload"),
            (
                ObjectType::CataloguePermissionsBundle,
                "decode permissions bundle payload",
            ),
            (
                ObjectType::CataloguePermissionsHead,
                "decode permissions head payload",
            ),
        ] {
            let data_dir = tempfile::TempDir::new().expect("temp data dir");
            let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
            let app_id = AppId::from_name(&format!("corrupt-{}", object_type.as_str()));
            let object_id = jazz::tools::ObjectId::new();
            let mut metadata = std::collections::HashMap::from([
                (MetadataKey::Type.to_string(), object_type.to_string()),
                (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
            ]);
            if object_type == ObjectType::CatalogueLens {
                let schema_hash = SchemaHash::compute(&dynamic_bootstrap_schema());
                metadata.insert(MetadataKey::SourceHash.to_string(), schema_hash.to_string());
                metadata.insert(MetadataKey::TargetHash.to_string(), schema_hash.to_string());
            }
            let entry = CatalogueEntry {
                object_id,
                metadata,
                content: vec![0],
            };
            write_raw_catalogue_entry(&catalogue_path, &entry);

            let result = ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await;
            let error = result
                .err()
                .expect("corrupt known catalogue payload must fail startup");
            assert!(
                error.contains("failed to read durable catalogue: Decode error"),
                "startup error retains the durable-catalogue context: {error}"
            );
            assert!(
                error.contains(object_type.as_str()) && error.contains(decode_context),
                "startup error identifies the corrupt known entry type and decoder: {error}"
            );
            assert!(
                error.contains(&object_id.to_string()),
                "startup error identifies the corrupt durable object: {error}"
            );

            let storage = jazz_storage_rocksdb::RocksDbStorage::open(&catalogue_path, &["default"])
                .expect("failed startup releases the catalogue RocksDB lock");
            jazz::db::block_on(storage.delete(
                "default".to_owned(),
                format!("cat:{}", object_id.uuid().simple()).into_bytes(),
            ))
            .expect("remove corrupt catalogue entry");
            drop(storage);
            ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("builder retries after repaired catalogue");
        }
    }

    #[test]
    fn owner_drop_stops_the_connector_while_peer_close_reconnects() {
        assert!(matches!(
            connected_transport_outcome(ServerUpstreamTerminalReason::NativeTransport(
                NativeTransportTerminal::OwnerDropped,
            )),
            EdgeConnectorOutcome::Stopped
        ));
        assert!(matches!(
            connected_transport_outcome(ServerUpstreamTerminalReason::Cancelled),
            EdgeConnectorOutcome::Stopped
        ));
        assert!(matches!(
            connected_transport_outcome(ServerUpstreamTerminalReason::NativeTransport(
                NativeTransportTerminal::PeerClosed("peer closed".to_owned()),
            )),
            EdgeConnectorOutcome::Reconnect(reason) if reason == "peer closed"
        ));
    }

    #[tokio::test]
    async fn persistent_builder_fails_when_known_catalogue_payload_has_trailing_garbage() {
        let schema = dynamic_bootstrap_schema();
        let schema_hash = SchemaHash::compute(&schema);
        let permissions = std::collections::HashMap::new();
        for (object_type, decode_context, mut content) in [
            (
                ObjectType::CatalogueSchema,
                "decode schema payload",
                crate::server::catalogue_payload_codec::encode_schema(&schema),
            ),
            (
                ObjectType::CatalogueLens,
                "decode lens payload",
                crate::server::catalogue_payload_codec::encode_lens_transform(&LensTransform::new()),
            ),
            (
                ObjectType::CataloguePermissionsBundle,
                "decode permissions bundle payload",
                crate::server::catalogue_payload_codec::encode_permissions_bundle(
                    schema_hash,
                    1,
                    None,
                    &permissions,
                ),
            ),
            (
                ObjectType::CataloguePermissionsHead,
                "decode permissions head payload",
                crate::server::catalogue_payload_codec::encode_permissions_head(
                    schema_hash,
                    1,
                    None,
                    jazz::tools::ObjectId::new(),
                ),
            ),
        ] {
            let data_dir = tempfile::TempDir::new().expect("temp data dir");
            let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
            let app_id = AppId::from_name(&format!("trailing-{}", object_type.as_str()));
            let object_id = jazz::tools::ObjectId::new();
            let mut metadata = std::collections::HashMap::from([
                (MetadataKey::Type.to_string(), object_type.to_string()),
                (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
            ]);
            if object_type == ObjectType::CatalogueLens {
                metadata.insert(MetadataKey::SourceHash.to_string(), schema_hash.to_string());
                metadata.insert(MetadataKey::TargetHash.to_string(), schema_hash.to_string());
            }
            content.push(0xff);
            write_raw_catalogue_entry(
                &catalogue_path,
                &CatalogueEntry {
                    object_id,
                    metadata,
                    content,
                },
            );

            let result = ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await;
            let error = result
                .err()
                .expect("known catalogue payload with trailing garbage must fail startup");
            assert!(
                error.contains("failed to read durable catalogue: Decode error"),
                "startup error retains durable-catalogue context: {error}"
            );
            assert!(
                error.contains(object_type.as_str())
                    && error.contains(decode_context)
                    && error.contains("trailing data after decoded payload"),
                "startup error identifies the known decoder and trailing payload data: {error}"
            );
            assert!(
                error.contains(&object_id.to_string()),
                "startup error identifies the corrupt durable object: {error}"
            );

            let storage = jazz_storage_rocksdb::RocksDbStorage::open(&catalogue_path, &["default"])
                .expect("failed startup releases the catalogue RocksDB lock");
            jazz::db::block_on(storage.delete(
                "default".to_owned(),
                format!("cat:{}", object_id.uuid().simple()).into_bytes(),
            ))
            .expect("remove corrupt catalogue entry");
            drop(storage);
            ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("builder retries after repaired catalogue");
        }
    }

    #[tokio::test]
    async fn persistent_builder_fails_when_catalogue_schema_publish_time_is_missing_or_invalid() {
        let schema = dynamic_bootstrap_schema();
        let schema_hash = SchemaHash::compute(&schema);
        for published_at in [None, Some("not-a-timestamp")] {
            let data_dir = tempfile::TempDir::new().expect("temp data dir");
            let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
            let app_id = AppId::from_name("corrupt-schema-publish-time");
            let object_id = jazz::tools::ObjectId::new();
            let mut metadata = std::collections::HashMap::from([
                (
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                ),
                (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
                (MetadataKey::SchemaHash.to_string(), schema_hash.to_string()),
            ]);
            if let Some(published_at) = published_at {
                metadata.insert(
                    MetadataKey::PublishedAt.to_string(),
                    published_at.to_owned(),
                );
            }
            write_raw_catalogue_entry(
                &catalogue_path,
                &CatalogueEntry {
                    object_id,
                    metadata,
                    content: crate::server::catalogue_payload_codec::encode_schema(&schema),
                },
            );

            let error = ServerBuilder::new(app_id)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .err()
                .expect("missing or malformed schema publication time must fail startup");
            assert!(
                error.contains("failed to read durable catalogue: Decode error")
                    && error.contains(ObjectType::CatalogueSchema.as_str())
                    && error.contains("published_at metadata")
                    && error.contains(&object_id.to_string()),
                "startup error identifies the corrupt schema metadata: {error}"
            );
        }
    }

    #[tokio::test]
    async fn persistent_builder_ignores_unknown_forward_compatible_catalogue_entries() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let catalogue_path = data_dir.path().join(CATALOGUE_ROCKSDB_DIR);
        let app_id = AppId::from_name("unknown-durable-catalogue-entry");
        let entry = CatalogueEntry {
            object_id: jazz::tools::ObjectId::new(),
            metadata: std::collections::HashMap::from([
                (
                    MetadataKey::Type.to_string(),
                    "future_catalogue_kind".to_owned(),
                ),
                (MetadataKey::AppId.to_string(), app_id.uuid().to_string()),
            ]),
            content: vec![0],
        };
        write_raw_catalogue_entry(&catalogue_path, &entry);

        ServerBuilder::new(app_id)
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("unknown forward-compatible catalogue entry does not block startup");
    }

    #[tokio::test]
    async fn dynamic_builder_starts_core_server_shell_from_rehydrated_catalogue_schema() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("dynamic-server-shell-rehydrate");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("todos")
                    .column("id", jazz::tools::public_schema::ColumnType::Uuid)
                    .column("title", jazz::tools::public_schema::ColumnType::Text)
                    .column("workspace_id", jazz::tools::public_schema::ColumnType::Uuid)
                    .branch_by("workspace_id"),
            )
            .build();
        let schema_hash = jazz::tools::public_schema::SchemaHash::compute(&schema);

        {
            let built = ServerBuilder::new(app_id)
                .with_schema(schema)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build fixed schema server");
            assert!(built.state.runtime().is_some());
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
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("build dynamic server from rehydrated catalogue");

        assert!(rebuilt.state.runtime().is_some());
        let restored = rebuilt
            .state
            .catalogue
            .known_schema(&rebuilt.state.catalogue_store, &schema_hash)
            .expect("read rehydrated schema")
            .expect("rehydrated schema is present");
        let restored_todos = restored
            .get(&jazz::tools::public_schema::TableName::new("todos"))
            .expect("restored todos table");
        assert_eq!(
            restored_todos.branch_by,
            vec![jazz::tools::public_schema::ColumnName::new("workspace_id")]
        );
    }

    #[tokio::test]
    async fn persistent_adapter_starts_core_server_shell_with_catalogue_storage_after_restart() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-restart");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("todos")
                    .column("id", jazz::tools::public_schema::ColumnType::Uuid)
                    .column("title", jazz::tools::public_schema::ColumnType::Text),
            )
            .build();

        let retained_state = {
            let built = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build RocksDB server with server shell");

            assert!(built.state.runtime().is_some());
            assert!(data_dir.path().join(CATALOGUE_ROCKSDB_DIR).exists());
            assert!(data_dir.path().join(SERVER_SHELL_ROCKSDB_DIR).exists());
            assert_eq!(
                built.shutdown().await,
                crate::server::ShutdownPhase::StorageClosed,
                "the public builder lifecycle must join the shell before its RocksDB path is reopened"
            );
            Arc::clone(&built.state)
        };
        assert!(
            retained_state.runtime().is_none(),
            "shutdown must retire the shell even if request/router state outlives BuiltServer"
        );

        let rebuilt = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("rebuild RocksDB server with server shell");

        assert!(rebuilt.state.runtime().is_some());
        assert!(data_dir.path().join(SERVER_SHELL_ROCKSDB_DIR).exists());
        rebuilt.shutdown().await;

        // Some direct builder consumers own only `BuiltServer` and use Rust
        // scope exit as their lifecycle. Its last-shell fallback must join as
        // well: this reopen has no timeout or sleep to mask an owner-thread
        // race.
        {
            let dropped = ServerBuilder::new(app_id)
                .with_schema(schema.clone())
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("build RocksDB server for direct-drop lifecycle");
            assert!(dropped.state.runtime().is_some());
        }

        let reopened_after_drop = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen RocksDB server after direct builder drop");
        assert!(reopened_after_drop.state.runtime().is_some());
        assert_eq!(
            reopened_after_drop.shutdown().await,
            crate::server::ShutdownPhase::StorageClosed
        );
        drop(retained_state);
    }

    #[tokio::test]
    async fn persistent_adapter_reopens_after_first_shutdown_waiter_is_aborted() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-aborted-shutdown");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("todos")
                    .column("id", jazz::tools::public_schema::ColumnType::Uuid),
            )
            .build();
        let built = ServerBuilder::new(app_id)
            .with_schema(schema.clone())
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
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
        while *phases.borrow_and_update() != crate::server::ShutdownPhase::DrainingConnections {
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
            crate::server::ShutdownPhase::StorageClosed
        );

        let reopened = ServerBuilder::new(app_id)
            .with_schema(schema)
            .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
            .with_storage(StorageBackend::Persistent {
                path: data_dir.path().to_path_buf(),
            })
            .build()
            .await
            .expect("reopen RocksDB after aborted shutdown waiter");
        assert_eq!(
            reopened.shutdown().await,
            crate::server::ShutdownPhase::StorageClosed
        );
    }

    #[test]
    fn persistent_adapter_shutdown_survives_initiating_runtime_drop() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let app_id = AppId::from_name("rocksdb-server-shell-foreign-shutdown");
        let schema = jazz::tools::public_schema::SchemaBuilder::new()
            .table(
                jazz::tools::public_schema::TableSchema::builder("todos")
                    .column("id", jazz::tools::public_schema::ColumnType::Uuid),
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
                        .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                        .with_storage(StorageBackend::Persistent {
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
                    != crate::server::ShutdownPhase::DrainingConnections
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
                crate::server::ShutdownPhase::StorageClosed
            );
            assert!(state.runtime().is_none());
            ServerBuilder::new(app_id)
                .with_schema(schema)
                .with_storage_factory(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory))
                .with_storage(StorageBackend::Persistent {
                    path: data_dir.path().to_path_buf(),
                })
                .build()
                .await
                .expect("reopen RocksDB after live shutdown")
        });
        assert_eq!(
            second_runtime.block_on(reopened.shutdown()),
            crate::server::ShutdownPhase::StorageClosed
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

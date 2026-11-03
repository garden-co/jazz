use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

use crate::middleware::AuthConfig;
use jazz::tools::AppId;
use jazz::tools::public_schema::Schema;

use super::{BuiltServer, ServerBuilder, ServerState, ShutdownPhase, StorageBackend};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const DEFAULT_APP_ID_STR: &str = "00000000-0000-0000-0000-000000000001";
/// An issuer for the sessions tests state as the app's backend, which
/// authenticated them (the core verifies no user tokens).
pub const TEST_JWT_ISSUER: &str = "urn:jazz:test";

/// Builder for configuring and starting a [`JazzServer`].
#[derive(Default)]
pub struct JazzServerBuilder {
    port: Option<u16>,
    app_id: Option<AppId>,
    data_dir: Option<PathBuf>,
    schema: Option<Schema>,
    persistent_storage: bool,
    admin_secret: Option<String>,
    backend_secret: Option<String>,
    auth_clock: Option<crate::middleware::auth::AuthClock>,
    trusted_workload: Option<crate::middleware::workload::TrustedWorkload>,
}

impl std::fmt::Debug for JazzServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JazzServerBuilder")
            .field("port", &self.port)
            .field("app_id", &self.app_id)
            .field("persistent_storage", &self.persistent_storage)
            .finish()
    }
}

impl JazzServerBuilder {
    /// Creates a builder with the default Jazz server test configuration.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn with_app_id(mut self, app_id: AppId) -> Self {
        self.app_id = Some(app_id);
        self
    }

    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_persistent_storage(mut self) -> Self {
        self.persistent_storage = true;
        self
    }

    pub fn with_admin_secret(mut self, secret: impl Into<String>) -> Self {
        self.admin_secret = Some(secret.into());
        self
    }

    pub fn with_backend_secret(mut self, secret: impl Into<String>) -> Self {
        self.backend_secret = Some(secret.into());
        self
    }

    #[cfg(feature = "test")]
    pub fn with_auth_clock(mut self, clock: crate::middleware::auth::TestClock) -> Self {
        self.auth_clock = Some(clock.into());
        self
    }

    /// Trust only this workload; the server then has no shared secrets.
    pub fn with_trusted_workload(
        mut self,
        workload: crate::middleware::workload::TrustedWorkload,
    ) -> Self {
        self.trusted_workload = Some(workload);
        self
    }

    pub async fn start(self) -> Result<JazzServer, String> {
        JazzServer::from_builder(self).await
    }
}

pub struct JazzServer {
    state: Arc<ServerState>,
    task: Option<JoinHandle<std::io::Result<()>>>,
    shutdown_task: Option<JoinHandle<ShutdownPhase>>,
    port: u16,
    app_id: AppId,
    data_dir: ServerDataDir,
    admin_secret: String,
    backend_secret: String,
    host: IpAddr,
    client_data_dirs: Mutex<Vec<OwnedTempDir>>,
    auth_clock: crate::middleware::auth::AuthClock,
}

impl JazzServer {
    pub const BACKEND_SECRET: &str = "backend-secret-for-integration-tests";
    pub const ADMIN_SECRET: &str = "admin-secret-for-integration-tests";

    /// Creates a builder for configuring a Jazz server before startup.
    pub fn builder() -> JazzServerBuilder {
        JazzServerBuilder::new()
    }

    pub async fn start() -> Result<Self, String> {
        Self::builder().start().await
    }

    pub async fn start_with_schema(schema: Schema) -> Result<Self, String> {
        Self::builder().with_schema(schema).start().await
    }

    async fn from_builder(builder: JazzServerBuilder) -> Result<Self, String> {
        let JazzServerBuilder {
            port,
            app_id,
            data_dir,
            schema,
            persistent_storage,
            admin_secret,
            backend_secret,
            auth_clock,
            trusted_workload,
        } = builder;

        let app_id = app_id.unwrap_or_else(AppId::random);
        let data_dir = if persistent_storage {
            ServerDataDir::persistent(data_dir)
        } else {
            ServerDataDir::in_memory()
        };
        let storage_data_dir = data_dir.path().to_path_buf();
        let admin_secret = admin_secret.unwrap_or_else(|| Self::ADMIN_SECRET.to_string());
        let backend_secret = backend_secret.unwrap_or_else(|| Self::BACKEND_SECRET.to_string());
        let auth_clock = auth_clock.unwrap_or_default();

        let auth_config = AuthConfig {
            allow_local_first_auth: true,
            backend_secret: trusted_workload.is_none().then(|| backend_secret.clone()),
            admin_secret: trusted_workload.is_none().then(|| admin_secret.clone()),
            trusted_workload,
            clock: auth_clock.clone(),
        };

        let server_builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
        let mut server_builder =
            apply_storage_mode(server_builder, storage_data_dir, persistent_storage);

        if let Some(schema) = schema {
            server_builder = server_builder.with_schema(schema);
        }
        let built = server_builder
            .build()
            .await
            .map_err(|error| error.to_string())?;

        let mut server = Self::from_built(
            built,
            port,
            None,
            app_id,
            data_dir,
            admin_secret,
            backend_secret,
        )
        .await?;
        server.auth_clock = auth_clock;
        Ok(server)
    }

    /// Create a Jazz server from an already-built router/state pair.
    ///
    /// This is used by bindings that need to construct their own
    /// [`ServerBuilder`] configuration while sharing the same server lifecycle
    /// and shutdown behavior as the Rust test server.
    pub async fn from_built(
        built: BuiltServer,
        port: Option<u16>,
        host: Option<String>,
        app_id: AppId,
        data_dir: ServerDataDir,
        admin_secret: String,
        backend_secret: String,
    ) -> Result<Self, String> {
        let host = host
            .unwrap_or_else(|| "127.0.0.1".to_owned())
            .parse::<IpAddr>()
            .map_err(|error| format!("invalid server host: {error}"))?;
        if host.is_unspecified() {
            return Err("invalid server host: wildcard addresses are not supported".to_owned());
        }
        let listener = tokio::net::TcpListener::bind(SocketAddr::new(host, port.unwrap_or(0)))
            .await
            .map_err(|error| format!("bind server listener: {error}"))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| format!("read server listener local address: {error}"))?;
        let port = local_addr.port();

        let (serve_shutdown_tx, serve_shutdown_rx) = oneshot::channel();
        let shutdown_state = built.state.clone();
        let shutdown_task = tokio::spawn(async move {
            shutdown_state.shutdown.wait_requested().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            let phase = shutdown_state.run_shutdown_finalization().await;
            let _ = serve_shutdown_tx.send(());
            phase
        });
        let task = tokio::spawn(async move {
            axum::serve(crate::tcp::low_latency_listener(listener), built.app)
                .with_graceful_shutdown(async {
                    let _ = serve_shutdown_rx.await;
                })
                .await
        });

        let server = Self {
            state: built.state,
            task: Some(task),
            shutdown_task: Some(shutdown_task),
            port,
            app_id,
            data_dir,
            admin_secret,
            host,
            backend_secret,
            client_data_dirs: Mutex::new(Vec::new()),
            auth_clock: crate::middleware::auth::AuthClock::default(),
        };
        server.wait_ready().await;
        Ok(server)
    }

    pub fn default_app_id() -> AppId {
        AppId::from_string(DEFAULT_APP_ID_STR).expect("parse default app id")
    }

    pub fn app_id(&self) -> AppId {
        self.app_id
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn base_url(&self) -> String {
        let host = match self.host {
            IpAddr::V4(host) => host.to_string(),
            IpAddr::V6(host) => format!("[{host}]"),
        };
        format!("http://{host}:{}", self.port)
    }

    pub fn admin_secret(&self) -> &str {
        &self.admin_secret
    }

    pub fn backend_secret(&self) -> &str {
        &self.backend_secret
    }

    /// Returns a clone of the shared `Arc<ServerState>` for in-process tests
    /// that need to inspect or drive internal server state.
    pub fn server_state(&self) -> std::sync::Arc<super::ServerState> {
        self.state.clone()
    }

    /// A client context of the app's backend, with in-memory storage and a
    /// fresh data directory kept until the server drops. A core admits only
    /// its backend; the client acts for a user with `for_session`, and
    /// `_user_id` only names what the test means it for.
    pub fn make_client_context_for_user(
        &self,
        schema: Schema,
        _user_id: impl AsRef<str>,
    ) -> jazz::tools::AppContext {
        let client_data_dir = OwnedTempDir::new("jazz-tools-testing-client");
        let data_dir = client_data_dir.path().to_path_buf();
        self.client_data_dirs
            .lock()
            .expect("lock test client data dirs")
            .push(client_data_dir);
        jazz::tools::AppContext {
            app_id: self.app_id,
            client_id: None,
            schema,
            server_url: self.base_url(),
            data_dir,
            storage: jazz::tools::ClientStorage::Memory,
            account_id: None,
            jwt_token: None,
            backend_secret: Some(self.backend_secret().to_string()),
            backend_session: None,
            admin_secret: None,
            migrations: self.published_migrations(),
        }
    }

    /// A client the app's backend opens for `user_id` (as `createJazz`
    /// relays a signed-in user's client): the backend secret, and the user's
    /// session with a `role: user` claim. Enroll it before connecting.
    pub fn make_user_context(
        &self,
        schema: Schema,
        user_id: impl AsRef<str>,
    ) -> jazz::tools::AppContext {
        let user_id = user_id.as_ref();
        let mut context = self.make_client_context_for_user(schema, user_id);
        context.backend_session = Some(
            jazz::tools::Session::new(TEST_JWT_ISSUER, user_id)
                .with_claims(serde_json::json!({ "role": "user" })),
        );
        context
    }

    /// The versions and lenses published to this server so far, as an app
    /// that deployed them ships them (SPEC 10.7).
    pub fn published_migrations(&self) -> jazz::db::Migrations {
        let state = &self.state;
        let store = &state.catalogue_store;
        let mut schemas = std::collections::HashMap::new();
        for hash in state
            .catalogue
            .known_schema_hashes(store)
            .expect("read published schema hashes")
        {
            if let Some(schema) = state
                .catalogue
                .known_schema(store, &hash)
                .expect("read published schema")
            {
                schemas.insert(hash, schema);
            }
        }
        let lenses = state
            .catalogue
            .known_lenses(store)
            .expect("read published lenses")
            .iter()
            .filter_map(|lens| {
                let source = schemas.get(&lens.source_hash)?;
                let target = schemas.get(&lens.target_hash)?;
                Some(
                    jazz::tools::schema_lens::runtime::compile_lens(lens, source, target)
                        .expect("lower a published lens"),
                )
            })
            .collect();
        let schemas = schemas
            .values()
            .map(|schema| jazz::schema::JazzSchema::new(schema).expect("lower a published schema"))
            .collect();
        jazz::db::Migrations { schemas, lenses }
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.path()
    }

    pub async fn shutdown(mut self) -> ShutdownPhase {
        self.state.shutdown.request_shutdown();
        let shutdown_budget = self.state.shutdown.timeout() * 2 + Duration::from_secs(5);

        let phase = if let Some(mut shutdown_task) = self.shutdown_task.take() {
            match tokio::time::timeout(shutdown_budget, &mut shutdown_task).await {
                Ok(Ok(phase)) => phase,
                Ok(Err(error)) => {
                    tracing::error!(%error, "embedded shutdown finalization task failed");
                    return ShutdownPhase::Failed;
                }
                Err(_) => {
                    shutdown_task.abort();
                    tracing::error!("embedded shutdown finalization timed out");
                    return ShutdownPhase::Failed;
                }
            }
        } else {
            self.state.shutdown.phase()
        };
        if phase != ShutdownPhase::StorageClosed {
            return phase;
        }

        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(shutdown_budget, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    tracing::error!(%error, "embedded server task failed during shutdown");
                    return ShutdownPhase::Failed;
                }
                Ok(Err(error)) => {
                    tracing::error!(%error, "embedded server task join failed during shutdown");
                    return ShutdownPhase::Failed;
                }
                Err(_) => {
                    task.abort();
                    let _ = tokio::time::timeout(Duration::from_millis(50), task).await;
                    tracing::error!("embedded server task timed out during shutdown");
                    return ShutdownPhase::Failed;
                }
            }
        }
        ShutdownPhase::StorageClosed
    }

    async fn wait_ready(&self) {
        let client = reqwest::Client::new();
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..80 {
            if client.get(&health_url).send().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("jazz server did not become ready in time");
    }
}

impl Drop for JazzServer {
    fn drop(&mut self) {
        self.state.shutdown.request_shutdown();
        if let Some(task) = self.shutdown_task.take() {
            task.abort();
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub struct ServerDataDir {
    path: PathBuf,
    _owned_temp: Option<OwnedTempDir>,
}

impl ServerDataDir {
    pub fn in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            _owned_temp: None,
        }
    }

    fn persistent(data_dir: Option<PathBuf>) -> Self {
        match data_dir {
            Some(path) => {
                std::fs::create_dir_all(&path).expect("create test server data dir");
                Self {
                    path,
                    _owned_temp: None,
                }
            }
            None => {
                let temp_dir = OwnedTempDir::new("jazz-tools-testing-server");
                let path = temp_dir.path().to_path_buf();
                Self {
                    path,
                    _owned_temp: Some(temp_dir),
                }
            }
        }
    }

    pub fn from_path(path: PathBuf) -> Self {
        if path.as_os_str().is_empty() {
            Self::in_memory()
        } else {
            Self {
                path,
                _owned_temp: None,
            }
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

struct OwnedTempDir {
    path: PathBuf,
}

impl OwnedTempDir {
    fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temp server dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for OwnedTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Applies the storage mode flags from [`JazzServerBuilder`] to a
/// [`ServerBuilder`].  Kept as a free function to avoid nested `#[cfg]`
/// blocks inside `from_builder`.
fn apply_storage_mode(
    builder: ServerBuilder,
    data_dir: PathBuf,
    persistent: bool,
) -> ServerBuilder {
    if persistent {
        builder.with_storage(StorageBackend::Persistent { path: data_dir })
    } else {
        builder.with_storage(StorageBackend::InMemory)
    }
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;

    use crate::server::ShutdownPhase;

    use super::*;

    #[tokio::test]
    async fn internal_shutdown_stops_jazz_server() {
        let server = JazzServer::start().await.expect("start test server");
        let base_url = server.base_url();
        let admin_secret = server.admin_secret().to_string();
        let client = reqwest::Client::new();

        let response = client
            .post(format!("{base_url}/internal/shutdown"))
            .header("X-Jazz-Admin-Secret", admin_secret)
            .send()
            .await
            .expect("shutdown request");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let mut saw_unavailable = false;
        for _ in 0..80 {
            match client.get(format!("{base_url}/health")).send().await {
                Ok(response) if response.status() == StatusCode::SERVICE_UNAVAILABLE => {
                    saw_unavailable = true;
                }
                Err(_) if saw_unavailable => {
                    assert_eq!(
                        server.server_state().shutdown.phase(),
                        ShutdownPhase::StorageClosed
                    );
                    return;
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        panic!("jazz server did not stop after internal shutdown");
    }
    #[tokio::test]
    async fn occupied_explicit_port_returns_contextual_error() {
        let blocker = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("reserve occupied server port");
        let port = blocker
            .local_addr()
            .expect("read occupied server port")
            .port();

        let result: Result<JazzServer, String> =
            JazzServer::builder().with_port(port).start().await;
        drop(blocker);

        let error = match result {
            Ok(_) => panic!("occupied explicit port should reject server startup"),
            Err(error) => error,
        };
        assert!(
            error.contains("bind") || error.contains("server listener"),
            "startup error should identify the server listener: {error}"
        );
    }

    #[tokio::test]
    async fn embedded_shutdown_reports_finalization_failure() {
        let app_id = JazzServer::default_app_id();
        let built = ServerBuilder::new(app_id)
            .with_storage(StorageBackend::InMemory)
            .with_shutdown_timeout(Duration::from_millis(1))
            .build()
            .await
            .expect("build embedded server");
        let active_request = built
            .state
            .shutdown
            .try_enter_app_request()
            .expect("enter active request");
        let server = JazzServer::from_built(
            built,
            None,
            None,
            app_id,
            ServerDataDir::in_memory(),
            JazzServer::ADMIN_SECRET.to_owned(),
            JazzServer::BACKEND_SECRET.to_owned(),
        )
        .await
        .expect("start embedded server");

        assert_eq!(server.shutdown().await, ShutdownPhase::Failed);
        drop(active_request);
    }
}

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, routing::get};
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use crate::AppContext;
use crate::middleware::AuthConfig;
use crate::query_manager::types::Schema;
use crate::schema_manager::AppId;

use super::{BuiltServer, ServerBuilder, ServerState, StorageBackend};
use crate::sync_manager::ClientId;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const DEFAULT_APP_ID_STR: &str = "00000000-0000-0000-0000-000000000001";
const JWT_KID: &str = "test-jwks-kid";
const JWT_SECRET: &str = "test-jwt-secret-for-integration";

/// Builder for configuring and starting a [`JazzServer`].
#[derive(Default)]
pub struct JazzServerBuilder {
    port: Option<u16>,
    app_id: Option<AppId>,
    data_dir: Option<PathBuf>,
    schema: Option<Schema>,
    persistent_storage: bool,
    sqlite_storage: bool,
    rocksdb_storage: bool,
    admin_secret: Option<String>,
    backend_secret: Option<String>,
    upstream_url: Option<String>,
    jwks_url: Option<String>,
    auth_clock: Option<crate::middleware::auth::AuthClock>,
    sync_tracer: Option<crate::sync_tracer::SyncTracer>,
}

impl std::fmt::Debug for JazzServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JazzServerBuilder")
            .field("port", &self.port)
            .field("app_id", &self.app_id)
            .field("persistent_storage", &self.persistent_storage)
            .field("has_tracer", &self.sync_tracer.is_some())
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

    /// Use SQLite as the server storage backend, regardless of which other
    /// storage features are compiled in.  Implies persistent storage.
    #[cfg(feature = "sqlite")]
    pub fn with_sqlite_storage(mut self) -> Self {
        self.sqlite_storage = true;
        self.persistent_storage = true;
        self
    }

    /// Use RocksDB as the server storage backend, regardless of which other
    /// storage features are compiled in.  Implies persistent storage.
    #[cfg(feature = "rocksdb")]
    pub fn with_rocksdb_storage(mut self) -> Self {
        self.rocksdb_storage = true;
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

    pub fn with_upstream_url(mut self, upstream_url: impl Into<String>) -> Self {
        self.upstream_url = Some(upstream_url.into());
        self
    }

    pub fn with_jwks_url(mut self, jwks_url: impl Into<String>) -> Self {
        self.jwks_url = Some(jwks_url.into());
        self
    }

    pub fn with_auth_clock(mut self, clock: crate::middleware::auth::TestClock) -> Self {
        self.auth_clock = Some(clock.into());
        self
    }

    pub fn with_tracer(mut self, tracer: crate::sync_tracer::SyncTracer) -> Self {
        self.sync_tracer = Some(tracer);
        self
    }

    pub async fn start(self) -> JazzServer {
        JazzServer::from_builder(self).await
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    iss: Option<String>,
    claims: JsonValue,
    exp: u64,
}

pub struct TestJwtOptions {
    pub expires_in: Duration,
    pub issuer: Option<String>,
}

impl Default for TestJwtOptions {
    fn default() -> Self {
        Self {
            expires_in: Duration::from_secs(3600),
            issuer: None,
        }
    }
}

pub struct TestJwtIssuer {
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl TestJwtIssuer {
    pub async fn start() -> Self {
        let app = Router::new().route("/jwks", get(jwks_handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind jwks server");
        let addr = listener.local_addr().expect("jwks local addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve jwks");
        });
        Self { addr, task }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}/jwks", self.addr)
    }

    pub fn jwt_for_user(sub: &str) -> String {
        Self::jwt_for_user_with_claims(sub, json!({"role": "user"}))
    }

    pub fn jwt_for_user_with_claims(sub: &str, claims: JsonValue) -> String {
        Self::jwt_for_user_with_options(sub, claims, TestJwtOptions::default())
    }

    pub fn jwt_for_user_with_options(
        sub: &str,
        claims: JsonValue,
        options: TestJwtOptions,
    ) -> String {
        Self::jwt_for_user_with_options_at(sub, claims, options, SystemTime::now())
    }

    fn jwt_for_user_with_options_at(
        sub: &str,
        claims: JsonValue,
        options: TestJwtOptions,
        now: SystemTime,
    ) -> String {
        let claims = JwtClaims {
            sub: sub.to_string(),
            iss: options.issuer,
            claims,
            exp: now
                .duration_since(UNIX_EPOCH)
                .expect("clock drift")
                .as_secs()
                + options.expires_in.as_secs(),
        };

        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(JWT_KID.to_string());

        encode(
            &header,
            &claims,
            &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
        )
        .expect("encode jwt")
    }
}

impl Drop for TestJwtIssuer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct JazzServer {
    state: Arc<ServerState>,
    task: Option<JoinHandle<()>>,
    shutdown_task: Option<JoinHandle<()>>,
    port: u16,
    app_id: AppId,
    data_dir: ServerDataDir,
    admin_secret: String,
    backend_secret: String,
    client_data_dirs: Mutex<Vec<OwnedTempDir>>,
    embedded_jwks_server: Option<TestJwtIssuer>,
    auth_clock: crate::middleware::auth::AuthClock,
}

impl JazzServer {
    pub const BACKEND_SECRET: &str = "backend-secret-for-integration-tests";
    pub const ADMIN_SECRET: &str = "admin-secret-for-integration-tests";

    /// Creates a builder for configuring a Jazz server before startup.
    pub fn builder() -> JazzServerBuilder {
        JazzServerBuilder::new()
    }

    pub async fn start() -> Self {
        Self::builder().start().await
    }

    pub async fn start_with_schema(schema: Schema) -> Self {
        Self::builder().with_schema(schema).start().await
    }

    async fn from_builder(builder: JazzServerBuilder) -> Self {
        let JazzServerBuilder {
            port,
            app_id,
            data_dir,
            schema,
            persistent_storage,
            sqlite_storage,
            rocksdb_storage,
            admin_secret,
            backend_secret,
            upstream_url,
            jwks_url,
            auth_clock,
            sync_tracer,
        } = builder;

        let app_id = app_id.unwrap_or_else(Self::default_app_id);
        let data_dir = if persistent_storage {
            ServerDataDir::persistent(data_dir)
        } else {
            ServerDataDir::in_memory()
        };
        let storage_data_dir = data_dir.path().to_path_buf();
        let (jwks_url, embedded_jwks_server) = match jwks_url {
            Some(jwks_url) => (jwks_url, None),
            None => {
                let jwks_server = TestJwtIssuer::start().await;
                let jwks_url = jwks_server.endpoint();
                (jwks_url, Some(jwks_server))
            }
        };

        let admin_secret = admin_secret.unwrap_or_else(|| Self::ADMIN_SECRET.to_string());
        let backend_secret = backend_secret.unwrap_or_else(|| Self::BACKEND_SECRET.to_string());
        let auth_clock = auth_clock.unwrap_or_default();

        let auth_config = AuthConfig {
            jwks_url: Some(jwks_url),
            allow_local_first_auth: true,
            backend_secret: Some(backend_secret.clone()),
            admin_secret: Some(admin_secret.clone()),
            clock: auth_clock.clone(),
            ..Default::default()
        };

        let mut server_builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
        if let Some(upstream_url) = upstream_url {
            server_builder = server_builder.with_upstream_url(upstream_url);
        }
        let mut server_builder = apply_storage_mode(
            server_builder,
            storage_data_dir,
            persistent_storage,
            sqlite_storage,
            rocksdb_storage,
        );

        if let Some(schema) = schema {
            server_builder = server_builder.with_schema(schema);
        }
        if let Some(tracer) = sync_tracer {
            server_builder = server_builder.with_sync_tracer(tracer);
        }
        let built = server_builder.build().await.expect("build test server");

        let mut server =
            Self::from_built(built, port, app_id, data_dir, admin_secret, backend_secret).await;
        server.embedded_jwks_server = embedded_jwks_server;
        server.auth_clock = auth_clock;
        server
    }

    /// Create a Jazz server from an already-built router/state pair.
    ///
    /// This is used by bindings that need to construct their own
    /// [`ServerBuilder`] configuration while sharing the same server lifecycle
    /// and shutdown behavior as the Rust test server.
    pub async fn from_built(
        built: BuiltServer,
        port: Option<u16>,
        app_id: AppId,
        data_dir: ServerDataDir,
        admin_secret: String,
        backend_secret: String,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port.unwrap_or(0)))
            .await
            .expect("bind server listener");
        let port = listener.local_addr().expect("local addr").port();

        let (serve_shutdown_tx, serve_shutdown_rx) = oneshot::channel();
        let shutdown_state = built.state.clone();
        let shutdown_task = tokio::spawn(async move {
            shutdown_state.shutdown.wait_requested().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_state.run_shutdown_finalization().await;
            let _ = serve_shutdown_tx.send(());
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, built.app)
                .with_graceful_shutdown(async {
                    let _ = serve_shutdown_rx.await;
                })
                .await
                .expect("serve jazz server");
        });

        let server = Self {
            state: built.state,
            task: Some(task),
            shutdown_task: Some(shutdown_task),
            port,
            app_id,
            data_dir,
            admin_secret,
            backend_secret,
            client_data_dirs: Mutex::new(Vec::new()),
            embedded_jwks_server: None,
            auth_clock: crate::middleware::auth::AuthClock::default(),
        };
        server.wait_ready().await;
        server
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
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn admin_secret(&self) -> &str {
        &self.admin_secret
    }

    pub fn backend_secret(&self) -> &str {
        &self.backend_secret
    }

    /// Returns a clone of the shared `Arc<ServerState>` for in-process tests
    /// that need to call internal server methods (e.g. `process_ws_client_frame`).
    pub fn server_state(&self) -> std::sync::Arc<super::ServerState> {
        self.state.clone()
    }

    /// Temporarily buffer server-to-client sync messages for the given client.
    pub fn block_messages_to(&self, client_id: ClientId) -> super::BlockedMessagesToClient {
        self.state.connection_event_hub.block_messages_to(client_id)
    }

    /// Set the client state TTL. Disconnected clients are reaped after this duration.
    pub async fn set_client_ttl(&self, ttl: Duration) {
        self.state.set_client_ttl(ttl).await;
    }

    /// Run one sweep iteration to reap expired disconnect candidates.
    pub async fn run_sweep_once(&self) -> Vec<ClientId> {
        self.state.run_sweep_once().await
    }

    /// Number of clients currently in the disconnect candidates list.
    pub async fn disconnect_candidate_count(&self) -> usize {
        self.state.disconnect_candidates.read().await.len()
    }

    fn require_built_in_jwt_helpers(&self) {
        if self.embedded_jwks_server.is_none() {
            panic!(
                "JazzServer uses an external JWKS URL; built-in JWT helpers are unavailable. Mint JWTs from your external JWKS test fixture instead."
            );
        }
    }

    fn make_base_client_context(&self, schema: Schema) -> AppContext {
        let client_data_dir = OwnedTempDir::new("jazz-tools-testing-client");
        let data_dir = client_data_dir.path().to_path_buf();
        self.client_data_dirs
            .lock()
            .expect("lock test client data dirs")
            .push(client_data_dir);

        AppContext {
            app_id: self.app_id,
            client_id: None,
            schema,
            server_url: self.base_url(),
            data_dir,
            storage: crate::ClientStorage::Memory,
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
            sync_tracer: None,
        }
    }

    pub fn make_client_context_for_user(
        &self,
        schema: Schema,
        user_id: impl AsRef<str>,
    ) -> AppContext {
        self.require_built_in_jwt_helpers();

        let now = UNIX_EPOCH + Duration::from_secs(self.auth_clock.now_seconds());
        let jwt_token = TestJwtIssuer::jwt_for_user_with_options_at(
            user_id.as_ref(),
            json!({"role": "user"}),
            TestJwtOptions::default(),
            now,
        );
        let mut context = self.make_base_client_context(schema);
        context.jwt_token = Some(jwt_token);
        context.backend_secret = Some(self.backend_secret().to_string());
        context
    }

    /// Creates a client context authenticated only with the backend secret.
    ///
    /// Use this context with [`crate::JazzClient::for_session`] when a test
    /// needs to perform an operation on behalf of a specific user.
    pub fn make_client_context_for_backend(&self, schema: Schema) -> AppContext {
        let mut context = self.make_base_client_context(schema);
        context.backend_secret = Some(self.backend_secret().to_string());
        context
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.path()
    }

    pub async fn shutdown(mut self) {
        self.state.shutdown.request_shutdown();
        let shutdown_budget = self.state.shutdown.timeout() * 2 + Duration::from_secs(5);

        let mut finalization_completed = false;
        if let Some(mut shutdown_task) = self.shutdown_task.take()
            && tokio::time::timeout(shutdown_budget, &mut shutdown_task)
                .await
                .is_ok()
        {
            finalization_completed = true;
        }

        if !finalization_completed {
            return;
        }

        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(shutdown_budget, &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = tokio::time::timeout(Duration::from_millis(50), task).await;
        }
    }

    async fn wait_ready(&self) {
        let client = reqwest::Client::new();
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..80 {
            if let Ok(response) = client.get(&health_url).send().await
                && response.status().is_success()
            {
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
    #[allow(unused_variables)] sqlite: bool,
    #[allow(unused_variables)] rocksdb: bool,
) -> ServerBuilder {
    #[cfg(feature = "sqlite")]
    if sqlite {
        return builder.with_storage(StorageBackend::Sqlite { path: data_dir });
    }
    #[cfg(feature = "rocksdb")]
    if rocksdb {
        return builder.with_storage(StorageBackend::RocksDb { path: data_dir });
    }
    if persistent {
        builder.with_storage(StorageBackend::Persistent { path: data_dir })
    } else {
        builder.with_storage(StorageBackend::InMemory)
    }
}

async fn jwks_handler() -> Json<JsonValue> {
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(JWT_SECRET.as_bytes());
    Json(json!({
        "keys": [
            {
                "kty": "oct",
                "kid": JWT_KID,
                "alg": "HS256",
                "k": encoded,
            }
        ]
    }))
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use reqwest::StatusCode;

    use super::*;

    #[tokio::test]
    async fn explicit_shutdown_stops_jazz_server() {
        let server = JazzServer::start().await;
        let base_url = server.base_url();
        let client = reqwest::Client::new();

        server.shutdown().await;

        for _ in 0..80 {
            if client
                .get(format!("{base_url}/health"))
                .send()
                .await
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        panic!("jazz server did not stop after explicit shutdown");
    }

    #[tokio::test]
    async fn default_jazz_server_keeps_built_in_jwt_helpers_enabled() {
        let server = JazzServer::start().await;
        let context = server.make_client_context_for_user(Schema::new(), "default-helper-user");

        assert!(context.jwt_token.is_some());
        assert!(context.admin_secret.is_none());

        server.shutdown().await;
    }

    /// The backend helper must not attach a JWT, because a JWT would make the
    /// server authenticate the connection as that user instead of as a backend.
    #[tokio::test]
    async fn backend_client_context_uses_backend_secret_without_jwt() {
        let server = JazzServer::start().await;
        let context = server.make_client_context_for_backend(Schema::new());

        assert!(context.jwt_token.is_none());
        assert_eq!(
            context.backend_secret.as_deref(),
            Some(server.backend_secret())
        );
        assert!(context.admin_secret.is_none());

        server.shutdown().await;
    }

    #[tokio::test]
    async fn external_jwks_url_disables_built_in_jwt_helpers() {
        let external_jwks = TestJwtIssuer::start().await;
        let server = JazzServer::builder()
            .with_jwks_url(external_jwks.endpoint())
            .start()
            .await;

        let health = reqwest::Client::new()
            .get(format!("{}/health", server.base_url()))
            .send()
            .await
            .expect("health request");
        assert_eq!(health.status(), StatusCode::OK);

        let panic = catch_unwind(AssertUnwindSafe(|| {
            server.make_client_context_for_user(Schema::new(), "external-helper-user")
        }))
        .expect_err("external JWKS mode should reject built-in JWT helpers");
        let message = if let Some(message) = panic.downcast_ref::<String>() {
            message.as_str()
        } else if let Some(message) = panic.downcast_ref::<&str>() {
            message
        } else {
            panic!("unexpected panic payload");
        };
        assert_eq!(
            message,
            "JazzServer uses an external JWKS URL; built-in JWT helpers are unavailable. Mint JWTs from your external JWKS test fixture instead."
        );

        server.shutdown().await;
    }
}

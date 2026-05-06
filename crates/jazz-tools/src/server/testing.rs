use std::path::{Path, PathBuf};
use std::sync::Mutex;
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

use super::hosted::HostedServer;
use super::{ServerBuilder, StorageBackend};
use crate::sync_manager::ClientId;

const DEFAULT_APP_ID_STR: &str = "00000000-0000-0000-0000-000000000001";
const JWT_KID: &str = "test-jwks-kid";
const JWT_SECRET: &str = "test-jwt-secret-for-integration";

/// Builder for configuring and starting a [`TestingServer`].
#[derive(Default)]
pub struct TestingServerBuilder {
    port: Option<u16>,
    app_id: Option<AppId>,
    data_dir: Option<PathBuf>,
    schema: Option<Schema>,
    persistent_storage: bool,
    sqlite_storage: bool,
    rocksdb_storage: bool,
    admin_secret: Option<String>,
    backend_secret: Option<String>,
    peer_secret: Option<String>,
    upstream_url: Option<String>,
    jwks_url: Option<String>,
    auth_clock: Option<crate::middleware::auth::AuthClock>,
    sync_tracer: Option<crate::sync_tracer::SyncTracer>,
}

impl std::fmt::Debug for TestingServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestingServerBuilder")
            .field("port", &self.port)
            .field("app_id", &self.app_id)
            .field("persistent_storage", &self.persistent_storage)
            .field("has_tracer", &self.sync_tracer.is_some())
            .finish()
    }
}

impl TestingServerBuilder {
    /// Creates a builder with the default test-server configuration.
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

    pub fn with_peer_secret(mut self, secret: impl Into<String>) -> Self {
        self.peer_secret = Some(secret.into());
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

    pub async fn start(self) -> TestingServer {
        TestingServer::start_from_builder(self).await
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    claims: JsonValue,
    exp: u64,
}

pub struct TestingJwksServer {
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl TestingJwksServer {
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
}

impl Drop for TestingJwksServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct TestingServer {
    hosted: HostedServer,
    app_id: AppId,
    admin_secret: String,
    backend_secret: String,
    default_client_user_id: String,
    client_data_dirs: Mutex<Vec<OwnedTempDir>>,
    _owned_data_dir: Option<OwnedTempDir>,
    embedded_jwks_server: Option<TestingJwksServer>,
    auth_clock: crate::middleware::auth::AuthClock,
}

impl TestingServer {
    pub const BACKEND_SECRET: &str = "backend-secret-for-integration-tests";
    pub const ADMIN_SECRET: &str = "admin-secret-for-integration-tests";

    /// Creates a builder for configuring a test server before startup.
    pub fn builder() -> TestingServerBuilder {
        TestingServerBuilder::new()
    }

    pub async fn start() -> Self {
        Self::builder().start().await
    }

    pub async fn start_with_schema(schema: Schema) -> Self {
        Self::builder().with_schema(schema).start().await
    }

    async fn start_from_builder(builder: TestingServerBuilder) -> Self {
        let TestingServerBuilder {
            port,
            app_id,
            data_dir,
            schema,
            persistent_storage,
            sqlite_storage,
            rocksdb_storage,
            admin_secret,
            backend_secret,
            peer_secret,
            upstream_url,
            jwks_url,
            auth_clock,
            sync_tracer,
        } = builder;

        let app_id = app_id.unwrap_or_else(Self::default_app_id);
        let (data_dir, owned_data_dir) = if persistent_storage {
            prepare_data_dir(data_dir)
        } else {
            (PathBuf::new(), None)
        };
        let (jwks_url, embedded_jwks_server) = match jwks_url {
            Some(jwks_url) => (jwks_url, None),
            None => {
                let jwks_server = TestingJwksServer::start().await;
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
            peer_secret,
            clock: auth_clock.clone(),
            ..Default::default()
        };

        let mut server_builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
        if let Some(upstream_url) = upstream_url {
            server_builder = server_builder.with_upstream_url(upstream_url);
        }
        let mut server_builder = apply_storage_mode(
            server_builder,
            data_dir.clone(),
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

        let hosted = HostedServer::start(
            built,
            port,
            app_id,
            data_dir,
            Some(admin_secret.clone()),
            Some(backend_secret.clone()),
        )
        .await;

        Self {
            hosted,
            app_id,
            admin_secret,
            backend_secret,
            default_client_user_id: format!("testing-user-{}", Uuid::new_v4()),
            client_data_dirs: Mutex::new(Vec::new()),
            _owned_data_dir: owned_data_dir,
            embedded_jwks_server,
            auth_clock,
        }
    }

    pub fn default_app_id() -> AppId {
        AppId::from_string(DEFAULT_APP_ID_STR).expect("parse default app id")
    }

    pub fn jwt_for_user(sub: &str) -> String {
        Self::jwt_for_user_with_claims(sub, json!({"role": "user"}))
    }

    pub fn jwt_for_user_with_claims(sub: &str, claims: JsonValue) -> String {
        Self::jwt_for_user_with_claims_at(sub, claims, SystemTime::now())
    }

    fn jwt_for_user_with_claims_at(sub: &str, claims: JsonValue, now: SystemTime) -> String {
        let claims = JwtClaims {
            sub: sub.to_string(),
            claims,
            exp: now
                .duration_since(UNIX_EPOCH)
                .expect("clock drift")
                .as_secs()
                + 3600,
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

    pub fn app_id(&self) -> AppId {
        self.app_id
    }

    pub fn port(&self) -> u16 {
        self.hosted.port
    }

    pub fn base_url(&self) -> String {
        self.hosted.base_url()
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
        self.hosted.state.clone()
    }

    /// Set the client state TTL. Disconnected clients are reaped after this duration.
    pub async fn set_client_ttl(&self, ttl: Duration) {
        self.hosted.state.set_client_ttl(ttl).await;
    }

    /// Run one sweep iteration to reap expired disconnect candidates.
    pub async fn run_sweep_once(&self) -> Vec<ClientId> {
        self.hosted.state.run_sweep_once().await
    }

    /// Number of clients currently in the disconnect candidates list.
    pub async fn disconnect_candidate_count(&self) -> usize {
        self.hosted.state.disconnect_candidates.read().await.len()
    }

    pub fn built_in_jwt_helpers_available(&self) -> bool {
        self.embedded_jwks_server.is_some()
    }

    pub fn uses_external_jwks(&self) -> bool {
        !self.built_in_jwt_helpers_available()
    }

    pub fn ensure_built_in_jwt_helpers_available(&self) -> Result<(), &'static str> {
        if self.built_in_jwt_helpers_available() {
            Ok(())
        } else {
            Err(
                "TestingServer uses an external JWKS URL; built-in JWT helpers are unavailable. Mint JWTs from your external JWKS test fixture instead.",
            )
        }
    }

    fn require_built_in_jwt_helpers(&self) {
        if let Err(message) = self.ensure_built_in_jwt_helpers_available() {
            panic!("{message}");
        }
    }

    pub fn make_client_context(&self, schema: Schema) -> AppContext {
        self.make_client_context_for_user(schema, &self.default_client_user_id)
    }

    pub fn make_client_context_for_user(
        &self,
        schema: Schema,
        user_id: impl AsRef<str>,
    ) -> AppContext {
        self.require_built_in_jwt_helpers();

        let client_data_dir = OwnedTempDir::new("jazz-tools-testing-client");
        let data_dir = client_data_dir.path().to_path_buf();
        self.client_data_dirs
            .lock()
            .expect("lock test client data dirs")
            .push(client_data_dir);

        let jwt_token = self.jwt_for_user_for_server_clock(user_id.as_ref());
        AppContext {
            app_id: self.app_id,
            client_id: None,
            schema,
            server_url: self.base_url(),
            data_dir,
            storage: crate::ClientStorage::Memory,
            jwt_token: Some(jwt_token),
            backend_secret: Some(self.backend_secret.clone()),
            admin_secret: None,
            sync_tracer: None,
        }
    }

    #[allow(dead_code)]
    pub fn data_dir(&self) -> &Path {
        &self.hosted.data_dir
    }

    pub async fn shutdown(mut self) {
        self.hosted.shutdown().await;
    }

    fn jwt_for_user_for_server_clock(&self, sub: &str) -> String {
        let now = UNIX_EPOCH + Duration::from_secs(self.auth_clock.now_seconds());
        Self::jwt_for_user_with_claims_at(sub, json!({"role": "user"}), now)
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

fn prepare_data_dir(data_dir: Option<PathBuf>) -> (PathBuf, Option<OwnedTempDir>) {
    match data_dir {
        Some(path) => {
            std::fs::create_dir_all(&path).expect("create test server data dir");
            (path, None)
        }
        None => {
            let temp_dir = OwnedTempDir::new("jazz-tools-testing-server");
            (temp_dir.path().to_path_buf(), Some(temp_dir))
        }
    }
}

/// Applies the storage mode flags from [`TestingServerBuilder`] to a
/// [`ServerBuilder`].  Kept as a free function to avoid nested `#[cfg]`
/// blocks inside `start_from_builder`.
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
    async fn default_testing_server_keeps_built_in_jwt_helpers_enabled() {
        let server = TestingServer::start().await;
        let context = server.make_client_context_for_user(Schema::new(), "default-helper-user");

        assert!(server.built_in_jwt_helpers_available());
        assert!(!server.uses_external_jwks());
        assert!(context.jwt_token.is_some());
        assert!(context.admin_secret.is_none());

        server.shutdown().await;
    }

    #[tokio::test]
    async fn external_jwks_url_disables_built_in_jwt_helpers() {
        let external_jwks = TestingJwksServer::start().await;
        let server = TestingServer::builder()
            .with_jwks_url(external_jwks.endpoint())
            .start()
            .await;

        assert!(!server.built_in_jwt_helpers_available());
        assert!(server.uses_external_jwks());
        assert_eq!(
            server.ensure_built_in_jwt_helpers_available(),
            Err(
                "TestingServer uses an external JWKS URL; built-in JWT helpers are unavailable. Mint JWTs from your external JWKS test fixture instead."
            )
        );

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
            "TestingServer uses an external JWKS URL; built-in JWT helpers are unavailable. Mint JWTs from your external JWKS test fixture instead."
        );

        server.shutdown().await;
    }
}

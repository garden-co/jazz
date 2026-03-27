use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, routing::get};
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::AppContext;
use crate::middleware::AuthConfig;
use crate::query_manager::types::Schema;
use crate::schema_manager::AppId;

use super::{ServerBuilder, ServerState};
use crate::sync_manager::ClientId;

const DEFAULT_APP_ID_STR: &str = "00000000-0000-0000-0000-000000000001";
const JWT_KID: &str = "test-jwks-kid";
const JWT_SECRET: &str = "test-jwt-secret-for-integration";

/// Builder for configuring and starting a [`TestingServer`].
#[derive(Debug, Default)]
pub struct TestingServerBuilder {
    port: Option<u16>,
    app_id: Option<AppId>,
    data_dir: Option<PathBuf>,
    schema: Option<Schema>,
    persistent_storage: bool,
    admin_secret: Option<String>,
    backend_secret: Option<String>,
    jwks_url: Option<String>,
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

    pub fn with_admin_secret(mut self, secret: impl Into<String>) -> Self {
        self.admin_secret = Some(secret.into());
        self
    }

    pub fn with_backend_secret(mut self, secret: impl Into<String>) -> Self {
        self.backend_secret = Some(secret.into());
        self
    }

    pub fn with_jwks_url(mut self, jwks_url: impl Into<String>) -> Self {
        self.jwks_url = Some(jwks_url.into());
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
    state: Arc<ServerState>,
    task: Option<tokio::task::JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    port: u16,
    client: reqwest::Client,
    app_id: AppId,
    data_dir: PathBuf,
    admin_secret: String,
    backend_secret: String,
    default_client_user_id: String,
    client_data_dirs: Mutex<Vec<OwnedTempDir>>,
    _owned_data_dir: Option<OwnedTempDir>,
    embedded_jwks_server: Option<TestingJwksServer>,
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
            admin_secret,
            backend_secret,
            jwks_url,
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

        let auth_config = AuthConfig {
            jwks_url: Some(jwks_url),
            allow_anonymous: true,
            allow_demo: true,
            backend_secret: Some(backend_secret.clone()),
            admin_secret: Some(admin_secret.clone()),
        };

        let mut server_builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
        server_builder = if persistent_storage {
            server_builder.with_persistent_storage(data_dir.to_string_lossy().into_owned())
        } else {
            server_builder.with_in_memory_storage()
        };

        if let Some(schema) = schema {
            server_builder = server_builder.with_schema(schema);
        }
        let built = server_builder.build().await.expect("build test server");

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port.unwrap_or(0)))
            .await
            .expect("bind test server listener");
        let port = listener.local_addr().expect("local addr").port();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, built.app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve jazz server");
        });

        let server = Self {
            state: built.state.clone(),
            task: Some(task),
            shutdown_tx: Some(shutdown_tx),
            port,
            client: reqwest::Client::new(),
            app_id,
            data_dir,
            admin_secret,
            backend_secret,
            default_client_user_id: format!("testing-user-{}", Uuid::new_v4()),
            client_data_dirs: Mutex::new(Vec::new()),
            _owned_data_dir: owned_data_dir,
            embedded_jwks_server,
        };
        server.wait_ready().await;
        server
    }

    pub fn default_app_id() -> AppId {
        AppId::from_string(DEFAULT_APP_ID_STR).expect("parse default app id")
    }

    pub fn jwt_for_user(sub: &str) -> String {
        Self::jwt_for_user_with_claims(sub, json!({"role": "user"}))
    }

    pub fn jwt_for_user_with_claims(sub: &str, claims: JsonValue) -> String {
        let claims = JwtClaims {
            sub: sub.to_string(),
            claims,
            exp: SystemTime::now()
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

    /// Set the client state TTL. Disconnected clients are reaped after this duration.
    pub fn set_client_ttl(&self, ttl: Duration) {
        self.state.set_client_ttl(ttl);
    }

    /// Run one sweep iteration to reap expired disconnect candidates.
    pub async fn run_sweep_once(&self) -> Vec<ClientId> {
        self.state.run_sweep_once().await
    }

    /// Number of clients currently in the disconnect candidates list.
    pub async fn disconnect_candidate_count(&self) -> usize {
        self.state.disconnect_candidates.read().await.len()
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

        AppContext {
            app_id: self.app_id,
            client_id: None,
            schema,
            server_url: self.base_url(),
            data_dir,
            storage: crate::ClientStorage::Memory,
            jwt_token: Some(Self::jwt_for_user(user_id.as_ref())),
            backend_secret: Some(self.backend_secret.clone()),
            admin_secret: Some(self.admin_secret.clone()),
        }
    }

    #[allow(dead_code)]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    async fn wait_ready(&self) {
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..80 {
            if let Ok(response) = self.client.get(&health_url).send().await
                && response.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("jazz-tools server did not become ready in time");
    }

    pub async fn shutdown(mut self) {
        self.state
            .runtime
            .flush()
            .await
            .expect("flush server runtime");
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(Duration::from_millis(500), &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = task.await;
        }
        self.state
            .runtime
            .with_storage(|storage| {
                storage.flush();
                storage.flush_wal();
                let _ = storage.close();
            })
            .expect("flush and close server storage");
        self.state
            .external_identity_store
            .close()
            .await
            .expect("close external identity store");
    }
}

impl Drop for TestingServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
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

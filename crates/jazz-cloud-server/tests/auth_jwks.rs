use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, extract::State, routing::get};
use base64::Engine;
use groove::query_manager::session::Session;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tempfile::TempDir;

const INTERNAL_API_SECRET: &str = "integration-internal-secret";
const SECRET_HASH_KEY: &str = "integration-secret-hash-key";

#[derive(Debug, Serialize)]
struct JwtClaims {
    sub: String,
    claims: Value,
    exp: u64,
}

#[derive(Debug, Deserialize)]
struct CreateAppResponse {
    app_id: String,
}

#[derive(Clone)]
struct JwksState {
    hits: Arc<AtomicUsize>,
    responses: Arc<tokio::sync::RwLock<Vec<Value>>>,
}

struct JwksServer {
    addr: SocketAddr,
    state: JwksState,
    task: tokio::task::JoinHandle<()>,
}

impl JwksServer {
    async fn start(responses: Vec<Value>) -> Self {
        let state = JwksState {
            hits: Arc::new(AtomicUsize::new(0)),
            responses: Arc::new(tokio::sync::RwLock::new(responses)),
        };

        let app = Router::new()
            .route("/jwks", get(jwks_handler))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind JWKS test server");
        let addr = listener.local_addr().expect("read JWKS server addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve JWKS test server");
        });

        Self { addr, state, task }
    }

    fn endpoint(&self) -> String {
        format!("http://{}/jwks", self.addr)
    }

    fn hits(&self) -> usize {
        self.state.hits.load(Ordering::SeqCst)
    }
}

impl Drop for JwksServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct TestServer {
    process: Child,
    port: u16,
    _data_dir: TempDir,
    client: Client,
}

impl TestServer {
    async fn start() -> Self {
        let data_dir = TempDir::new().expect("create temp data dir");
        let port = get_free_port();

        let process = Command::new(env!("CARGO_BIN_EXE_jazz-cloud-server"))
            .args([
                "--port",
                &port.to_string(),
                "--data-root",
                data_dir.path().to_str().expect("temp dir path"),
                "--internal-api-secret",
                INTERNAL_API_SECRET,
                "--secret-hash-key",
                SECRET_HASH_KEY,
                "--worker-threads",
                "1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn jazz-cloud-server");

        let server = Self {
            process,
            port,
            _data_dir: data_dir,
            client: Client::new(),
        };

        server.wait_ready().await;
        server
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    async fn wait_ready(&self) {
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..60 {
            if let Ok(response) = self.client.get(&health_url).send().await
                && response.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("jazz-cloud-server did not become ready in time");
    }

    async fn create_app(&self, jwks_endpoint: &str) -> CreateAppResponse {
        self.create_app_with_secrets(jwks_endpoint, None, None)
            .await
    }

    async fn create_app_with_secrets(
        &self,
        jwks_endpoint: &str,
        backend_secret: Option<&str>,
        admin_secret: Option<&str>,
    ) -> CreateAppResponse {
        let mut payload = json!({
            "app_name": "integration-app",
            "jwks_endpoint": jwks_endpoint,
        });
        if let Some(secret) = backend_secret {
            payload["backend_secret"] = Value::String(secret.to_string());
        }
        if let Some(secret) = admin_secret {
            payload["admin_secret"] = Value::String(secret.to_string());
        }

        let response = self
            .client
            .post(format!("{}/internal/apps", self.base_url()))
            .header("X-Jazz-Internal-Secret", INTERNAL_API_SECRET)
            .json(&payload)
            .send()
            .await
            .expect("create app request");

        let status = response.status();
        let text = response.text().await.expect("read create app response");
        assert!(
            status.is_success(),
            "create app failed: status={status}, body={text}"
        );

        serde_json::from_str(&text).expect("parse create app response")
    }

    async fn sync_with_bearer(&self, app_id: &str, token: &str) -> reqwest::Response {
        self.client
            .post(format!("{}/apps/{app_id}/sync", self.base_url()))
            .header("Authorization", format!("Bearer {token}"))
            .json(&sync_body())
            .send()
            .await
            .expect("sync request")
    }

    async fn sync_with_backend_session(
        &self,
        app_id: &str,
        backend_secret: Option<&str>,
        session_user: &str,
    ) -> reqwest::Response {
        let mut request = self
            .client
            .post(format!("{}/apps/{app_id}/sync", self.base_url()))
            .header("X-Jazz-Session", encode_session(session_user))
            .json(&sync_body());

        if let Some(secret) = backend_secret {
            request = request.header("X-Jazz-Backend-Secret", secret);
        }

        request.send().await.expect("sync request")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

async fn jwks_handler(State(state): State<JwksState>) -> Json<Value> {
    let idx = state.hits.fetch_add(1, Ordering::SeqCst);
    let responses = state.responses.read().await;

    let body = responses
        .get(idx)
        .cloned()
        .or_else(|| responses.last().cloned())
        .unwrap_or_else(|| json!({ "keys": [] }));

    Json(body)
}

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind port 0");
    listener.local_addr().expect("port local_addr").port()
}

fn sync_body() -> Value {
    json!({
        "client_id": "01234567-89ab-cdef-0123-456789abcdef",
        "payload": {
            "ObjectUpdated": {
                "object_id": "01234567-89ab-cdef-0123-456789abcdef",
                "metadata": null,
                "branch_name": "main",
                "commits": []
            }
        }
    })
}

fn encode_session(user_id: &str) -> String {
    let session = Session::new(user_id);
    let json = serde_json::to_string(&session).expect("serialize session");
    base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
}

fn hs256_jwks(kid: &str, secret: &str) -> Value {
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret.as_bytes());
    json!({
        "keys": [
            {
                "kty": "oct",
                "kid": kid,
                "alg": "HS256",
                "k": encoded,
            }
        ]
    })
}

fn make_jwt(sub: &str, kid: &str, secret: &str) -> String {
    let claims = JwtClaims {
        sub: sub.to_string(),
        claims: json!({ "role": "user" }),
        exp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_secs()
            + 3600,
    };

    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(kid.to_string());

    encode(
        &header,
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("encode JWT for test")
}

#[tokio::test]
async fn valid_bearer_jwt_is_accepted_with_cached_jwks() {
    let jwks_server = JwksServer::start(vec![hs256_jwks("kid-valid", "secret-valid")]).await;
    let server = TestServer::start().await;
    let app = server.create_app(&jwks_server.endpoint()).await;

    let token = make_jwt("user-valid", "kid-valid", "secret-valid");

    let first = server.sync_with_bearer(&app.app_id, &token).await;
    assert_ne!(
        first.status(),
        StatusCode::UNAUTHORIZED,
        "first sync should pass auth"
    );
    assert_eq!(
        jwks_server.hits(),
        1,
        "first request should fetch JWKS exactly once"
    );

    let second = server.sync_with_bearer(&app.app_id, &token).await;
    assert_ne!(
        second.status(),
        StatusCode::UNAUTHORIZED,
        "second sync should pass auth"
    );
    assert_eq!(
        jwks_server.hits(),
        1,
        "second request should use cached JWKS and not refetch"
    );
}

#[tokio::test]
async fn unknown_kid_triggers_single_refresh_then_succeeds() {
    let jwks_server = JwksServer::start(vec![
        hs256_jwks("kid-old", "secret-old"),
        hs256_jwks("kid-new", "secret-new"),
    ])
    .await;
    let server = TestServer::start().await;
    let app = server.create_app(&jwks_server.endpoint()).await;

    let token = make_jwt("user-refresh", "kid-new", "secret-new");

    let response = server.sync_with_bearer(&app.app_id, &token).await;
    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "sync should pass after one JWKS refresh"
    );
    assert_eq!(
        jwks_server.hits(),
        2,
        "unknown kid should trigger exactly one forced JWKS refresh"
    );
}

#[tokio::test]
async fn bad_signature_stays_unauthorized_after_refresh_retry() {
    let jwks_server = JwksServer::start(vec![
        hs256_jwks("kid-signature", "good-secret"),
        hs256_jwks("kid-signature", "good-secret"),
    ])
    .await;
    let server = TestServer::start().await;
    let app = server.create_app(&jwks_server.endpoint()).await;

    let token = make_jwt("user-invalid", "kid-signature", "wrong-secret");

    let response = server.sync_with_bearer(&app.app_id, &token).await;
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "invalid signature must remain unauthorized after refresh retry"
    );
    assert_eq!(
        jwks_server.hits(),
        2,
        "signature failure should trigger one refresh retry"
    );
}

#[tokio::test]
async fn backend_session_auth_requires_secret_and_accepts_valid_secret() {
    let jwks_server = JwksServer::start(vec![hs256_jwks("kid-valid", "secret-valid")]).await;
    let server = TestServer::start().await;
    let app = server
        .create_app_with_secrets(
            &jwks_server.endpoint(),
            Some("backend-secret-1"),
            Some("admin-secret-1"),
        )
        .await;

    let missing_secret = server
        .sync_with_backend_session(&app.app_id, None, "backend-user")
        .await;
    assert_eq!(missing_secret.status(), StatusCode::UNAUTHORIZED);

    let valid_secret = server
        .sync_with_backend_session(&app.app_id, Some("backend-secret-1"), "backend-user")
        .await;
    assert_ne!(valid_secret.status(), StatusCode::UNAUTHORIZED);
}

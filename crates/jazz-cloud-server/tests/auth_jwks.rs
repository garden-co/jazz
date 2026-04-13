use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, extract::State, routing::get};
use base64::Engine;
use jazz_tools::commit::CommitId;
use jazz_tools::query_manager::session::Session;
use jazz_tools::row_histories::{RowState, StoredRowVersion};
use jazz_tools::sync_manager::{ClientId, SyncPayload};
use jazz_tools::transport_protocol::SyncBatchRequest;
use jazz_tools::{ObjectId, metadata::RowProvenance};
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
    #[serde(skip_serializing_if = "Option::is_none")]
    iss: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    jazz_principal_id: Option<String>,
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
        Self::start_with_env(vec![]).await
    }

    async fn start_with_env(extra_env: Vec<(&str, String)>) -> Self {
        let data_dir = TempDir::new().expect("create temp data dir");
        let port = get_free_port();

        let mut cmd = Command::new(env!("CARGO_BIN_EXE_jazz-cloud-server"));
        cmd.args([
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
        .envs(extra_env)
        .stdout(Stdio::null());

        if std::env::var("JAZZ_TEST_SERVER_LOGS").is_ok() {
            cmd.stderr(Stdio::inherit());
        } else {
            cmd.stderr(Stdio::null());
        }

        let process = cmd.spawn().expect("spawn jazz-cloud-server");

        let mut server = Self {
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

    async fn wait_ready(&mut self) {
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..200 {
            if let Some(status) = self.process.try_wait().expect("poll jazz-cloud-server") {
                panic!("jazz-cloud-server exited before becoming ready: {status}");
            }
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
        self.create_app_with_config(Some(jwks_endpoint), None, None, None, None)
            .await
    }

    async fn create_app_with_secrets(
        &self,
        jwks_endpoint: &str,
        backend_secret: Option<&str>,
        admin_secret: Option<&str>,
    ) -> CreateAppResponse {
        self.create_app_with_config(
            Some(jwks_endpoint),
            backend_secret,
            admin_secret,
            None,
            None,
        )
        .await
    }

    async fn create_app_with_config(
        &self,
        jwks_endpoint: Option<&str>,
        backend_secret: Option<&str>,
        admin_secret: Option<&str>,
        jwks_cache_ttl_secs: Option<u64>,
        jwks_max_stale_secs: Option<u64>,
    ) -> CreateAppResponse {
        let mut payload = json!({
            "app_name": "integration-app",
        });
        if let Some(endpoint) = jwks_endpoint {
            payload["jwks_endpoint"] = Value::String(endpoint.to_string());
        }
        if let Some(secret) = backend_secret {
            payload["backend_secret"] = Value::String(secret.to_string());
        }
        if let Some(secret) = admin_secret {
            payload["admin_secret"] = Value::String(secret.to_string());
        }
        if let Some(ttl_secs) = jwks_cache_ttl_secs {
            payload["jwks_cache_ttl_secs"] = Value::Number(ttl_secs.into());
        }
        if let Some(max_stale_secs) = jwks_max_stale_secs {
            payload["jwks_max_stale_secs"] = Value::Number(max_stale_secs.into());
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

fn auth_jwks_test_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

fn sync_body() -> SyncBatchRequest {
    let row_id = ObjectId::new();
    let row = StoredRowVersion::new(
        row_id,
        "main",
        Vec::<CommitId>::new(),
        b"alice".to_vec(),
        RowProvenance::for_insert(row_id.to_string(), 1_000),
        Default::default(),
        RowState::VisibleDirect,
        None,
    );

    SyncBatchRequest {
        payloads: vec![SyncPayload::RowVersionCreated {
            metadata: None,
            row,
        }],
        client_id: ClientId::new(),
    }
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
    make_jwt_with_options(sub, kid, secret, Some("https://issuer.test"), None)
}

fn make_jwt_with_options(
    sub: &str,
    kid: &str,
    secret: &str,
    issuer: Option<&str>,
    principal_id: Option<&str>,
) -> String {
    let claims = JwtClaims {
        sub: sub.to_string(),
        iss: issuer.map(str::to_string),
        jazz_principal_id: principal_id.map(str::to_string),
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
    let _guard = auth_jwks_test_guard();
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
    let _guard = auth_jwks_test_guard();
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
    let _guard = auth_jwks_test_guard();
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

/// Rapid requests with different unknown kids should not trigger unbounded
/// JWKS fetches. After the first forced refresh, subsequent unknown-kid
/// requests within the cooldown window should reuse the cached keyset
/// rather than hammering the IdP endpoint.
///
/// ```text
///  req(kid-0)          req(kid-1)          req(kid-2)
///    |                    |                    |
///    v                    v                    v
///  load(1) ──> miss    cache hit            cache hit
///  no match →          no match →           no match →
///  refresh(2)          cooldown ──>         cooldown ──>
///  no match → 401      use cached → 401    use cached → 401
/// ```
#[tokio::test]
async fn rapid_unknown_kids_do_not_trigger_unbounded_refreshes() {
    let _guard = auth_jwks_test_guard();
    let jwks_server = JwksServer::start(vec![hs256_jwks("kid-stable", "secret-stable")]).await;
    let server = TestServer::start().await;
    let app = server.create_app(&jwks_server.endpoint()).await;

    // Send 5 requests with different fabricated kids in rapid succession.
    for i in 0..5 {
        let token = make_jwt(
            "user-dos",
            &format!("kid-fabricated-{i}"),
            "irrelevant-secret",
        );

        let response = server.sync_with_bearer(&app.app_id, &token).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // Without cooldown: 1 (first load) + 5 (one forced refresh per request) = 6
    // With cooldown:    1 (first load) + 1 (first refresh, then cooldown) = 2
    assert_eq!(
        jwks_server.hits(),
        2,
        "rapid unknown-kid requests should trigger at most one refresh within the cooldown window"
    );
}

/// When the JWKS endpoint goes down after the cache TTL expires, requests
/// with valid JWTs should still succeed using the stale cached keyset
/// rather than failing with an auth error.
#[tokio::test]
async fn stale_jwks_served_when_endpoint_goes_down_after_ttl_expiry() {
    let _guard = auth_jwks_test_guard();
    // Response 1: valid key. Response 2+: empty keys (fetch_jwks rejects these).
    let jwks_server = JwksServer::start(vec![
        hs256_jwks("kid-stale", "secret-stale"),
        json!({ "keys": [] }),
    ])
    .await;
    let server = TestServer::start().await;
    let jwks_endpoint = jwks_server.endpoint();
    let app = server
        .create_app_with_config(Some(&jwks_endpoint), None, None, None, None, Some(1), None)
        .await;

    let token = make_jwt("user-stale", "kid-stale", "secret-stale");

    // First request: fetches JWKS (hit 1), validates OK.
    let first = server.sync_with_bearer(&app.app_id, &token).await;
    assert_ne!(
        first.status(),
        StatusCode::UNAUTHORIZED,
        "first request should succeed with cached JWKS"
    );

    // Wait for TTL to expire.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Second request: TTL expired, fetch fails (empty keys), should serve stale.
    let second = server.sync_with_bearer(&app.app_id, &token).await;
    assert_ne!(
        second.status(),
        StatusCode::UNAUTHORIZED,
        "request should succeed with stale JWKS when endpoint is down"
    );
}

/// Stale keysets should not be served forever. Once the entry is older
/// than TTL + max_stale, the fallback is refused and the request fails.
#[tokio::test]
async fn stale_jwks_refused_after_max_stale_expires() {
    let _guard = auth_jwks_test_guard();
    let jwks_server = JwksServer::start(vec![
        hs256_jwks("kid-expiry", "secret-expiry"),
        json!({ "keys": [] }),
    ])
    .await;
    // TTL=1s, max_stale=1s → total window = 2s.
    let server = TestServer::start().await;
    let jwks_endpoint = jwks_server.endpoint();
    let app = server
        .create_app_with_config(
            Some(&jwks_endpoint),
            None,
            None,
            None,
            None,
            Some(1),
            Some(1),
        )
        .await;

    let token = make_jwt("user-expiry", "kid-expiry", "secret-expiry");

    // First request: validates OK.
    let first = server.sync_with_bearer(&app.app_id, &token).await;
    assert_ne!(first.status(), StatusCode::UNAUTHORIZED);

    // Wait beyond TTL + max_stale (2s total).
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // Request should now fail — stale keyset is too old to serve.
    let expired = server.sync_with_bearer(&app.app_id, &token).await;
    assert_eq!(
        expired.status(),
        StatusCode::UNAUTHORIZED,
        "stale keyset beyond max_stale should not be served"
    );
}

#[tokio::test]
async fn backend_session_auth_requires_secret_and_accepts_valid_secret() {
    let _guard = auth_jwks_test_guard();
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

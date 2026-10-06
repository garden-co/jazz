use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use jazz::account_registry::AccountId;
use jazz::ids::AuthorSubject;
use jazz::tools::public_schema::Schema;
use jazz::wire::{
    FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, WireError, WireErrorCode, WireFrame,
    WireHello, WirePeerRole, WireRetry, decode_frame, decode_websocket_frame_batch, encode_frame,
    encode_websocket_frame_batch,
};
use jazz_server::{JazzServer, TEST_JWT_AUDIENCE, TEST_JWT_ISSUER};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use uuid::Uuid;

const SIGNING_KEY: &str = "synthetic-jwks-retry-test-signing-key";
const KEY_ID: &str = "synthetic-jwks-retry-key";
const STEP_TIMEOUT: Duration = Duration::from_secs(5);
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn bounded<T>(operation: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(STEP_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("test fixture timed out while {operation}"))
}

struct JwksService {
    addr: SocketAddr,
    available: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl JwksService {
    async fn start() -> Self {
        let available = Arc::new(AtomicBool::new(false));
        let router = Router::new()
            .route("/jwks", get(jwks))
            .with_state(available.clone());
        let listener = bounded(
            "binding the local JWKS service",
            tokio::net::TcpListener::bind("127.0.0.1:0"),
        )
        .await
        .expect("bind local JWKS service");
        let addr = listener.local_addr().expect("JWKS address");
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve JWKS");
        });
        Self {
            addr,
            available,
            task,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}/jwks", self.addr)
    }
}

impl Drop for JwksService {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn jwks(State(available): State<Arc<AtomicBool>>) -> axum::response::Response {
    if !available.load(Ordering::SeqCst) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Json(json!({
        "keys": [{
            "kty": "oct",
            "kid": KEY_ID,
            "alg": "HS256",
            "k": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(SIGNING_KEY),
        }],
    }))
    .into_response()
}

fn token(subject: &str, signing_key: &str) -> String {
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(KEY_ID.to_owned());
    encode(
        &header,
        &json!({
            "sub": subject,
            "iss": TEST_JWT_ISSUER,
            "aud": TEST_JWT_AUDIENCE,
            "exp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600,
        }),
        &EncodingKey::from_secret(signing_key.as_bytes()),
    )
    .expect("mint synthetic JWT")
}

async fn connect(server: &JazzServer, subject: &str, token: &str, account: AccountId) -> Socket {
    let identity = AuthorSubject::authenticated(TEST_JWT_ISSUER, subject)
        .expect("synthetic issuer and subject")
        .with_account(account);
    let url = format!(
        "{}/apps/{}/ws",
        server.base_url().replace("http://", "ws://"),
        server.app_id(),
    );
    let (mut socket, _) = bounded("opening the WebSocket", connect_async(url))
        .await
        .expect("open WebSocket");
    bounded(
        "sending the auth prelude",
        socket.send(Message::Binary(
            json!({
                "peer_identity": identity.canonical(),
                "auth": { "jwt_token": token },
            })
            .to_string()
            .into_bytes()
            .into(),
        )),
    )
    .await
    .expect("send auth prelude");
    socket
}

async fn receive(socket: &mut Socket) -> WireFrame {
    let message = bounded("receiving the handshake response", socket.next())
        .await
        .expect("handshake frame")
        .expect("read WebSocket response");
    let Message::Binary(bytes) = message else {
        panic!("expected binary handshake frame, got {message:?}");
    };
    let frames = decode_websocket_frame_batch(&bytes).expect("decode WebSocket batch");
    assert_eq!(frames.len(), 1, "one handshake outcome");
    decode_frame(&frames[0]).expect("decode handshake outcome")
}

#[derive(Deserialize)]
struct Assignment {
    account: Uuid,
}

/// Mallory's malformed JWT remains a permanent denial even when Alice's issuer
/// is unavailable. An outage must not hide invalid payload or signature encoding.
///
/// mallory -> core -> malformed JWT -> never retry
#[tokio::test]
async fn malformed_jwt_is_a_permanent_denial_during_jwks_outage() {
    let jwks = JwksService::start().await;
    let server = bounded(
        "starting the isolated server",
        JazzServer::builder()
            .with_jwks_url(jwks.endpoint())
            .with_schema(Schema::new())
            .start(),
    )
    .await
    .expect("start isolated server");
    let valid = token("mallory", SIGNING_KEY);
    let parts: Vec<_> = valid.split('.').collect();
    let invalid_json = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("not-json");
    for (case, malformed) in [
        ("payload encoding", format!("{}.!.{}", parts[0], parts[2])),
        (
            "payload JSON",
            format!("{}.{invalid_json}.{}", parts[0], parts[2]),
        ),
        ("signature encoding", format!("{}.{}.!", parts[0], parts[1])),
    ] {
        let mut mallory = connect(&server, "mallory", &malformed, AccountId(Uuid::new_v4())).await;
        let denial = receive(&mut mallory).await;
        assert!(
            matches!(
                denial,
                WireFrame::Error(WireError {
                    code: WireErrorCode::AuthFailed,
                    retry: WireRetry::Never,
                    ..
                })
            ),
            "malformed {case} must be a permanent denial during an outage: {denial:?}",
        );
    }
    bounded("shutting down the isolated server", server.shutdown()).await;
}

/// Alice receives a retryable denial while her issuer's JWKS service is down,
/// reconnects with the same token after recovery, and Mallory's bad signature
/// remains a permanent credential denial.
///
/// alice -> core -> JWKS 500 -> retry later
///       -> core -> restored JWKS -> authenticated hello
/// mallory -> core -> bad signature -> never retry
#[tokio::test]
async fn jwks_outage_is_retryable_and_recovery_preserves_credential_denials() {
    let jwks = JwksService::start().await;
    let server = bounded(
        "starting the isolated server",
        JazzServer::builder()
            .with_jwks_url(jwks.endpoint())
            .with_schema(Schema::new())
            .start(),
    )
    .await
    .expect("start isolated server");
    let mut malformed = connect(&server, "mallory", "not-a-jwt", AccountId(Uuid::new_v4())).await;
    assert!(matches!(
        receive(&mut malformed).await,
        WireFrame::Error(WireError {
            code: WireErrorCode::AuthFailed,
            retry: WireRetry::Never,
            ..
        })
    ));
    let alice_token = token("alice", SIGNING_KEY);
    let mut alice = connect(&server, "alice", &alice_token, AccountId(Uuid::new_v4())).await;
    let outage = receive(&mut alice).await;
    assert!(
        matches!(
            &outage,
            WireFrame::Error(WireError {
                code: WireErrorCode::NotReady,
                retry: WireRetry::Later,
                ..
            })
        ),
        "an unavailable key service must not be a permanent credential denial: {outage:?}",
    );

    jwks.available.store(true, Ordering::SeqCst);
    // Enrol Alice through the public API, not by editing the registry.
    let assignment: Assignment = reqwest::Client::builder()
        .timeout(STEP_TIMEOUT)
        .build()
        .expect("build bounded enrolment client")
        .post(format!(
            "{}/apps/{}/accounts/login-or-register",
            server.base_url(),
            server.app_id(),
        ))
        .bearer_auth(&alice_token)
        .send()
        .await
        .expect("enrol Alice after JWKS recovery")
        .error_for_status()
        .expect("Alice's unchanged token is valid")
        .json()
        .await
        .expect("public account assignment");
    let mut alice = connect(
        &server,
        "alice",
        &alice_token,
        AccountId(assignment.account),
    )
    .await;
    let hello = WireFrame::Hello(WireHello::current(
        WirePeerRole::Client,
        FEATURE_STRUCTURED_ERRORS | FEATURE_SYNC_MESSAGE_PAYLOAD,
    ));
    bounded(
        "sending the client hello",
        alice.send(Message::Binary(
            encode_websocket_frame_batch(&[encode_frame(&hello).expect("encode client hello")])
                .expect("encode hello batch")
                .into(),
        )),
    )
    .await
    .expect("send client hello");
    assert!(matches!(receive(&mut alice).await, WireFrame::Hello(_)));
    bounded("closing Alice's session", alice.close(None))
        .await
        .expect("close Alice's session");

    let mallory_token = token("mallory", "synthetic-wrong-signing-key");
    let mut mallory = connect(
        &server,
        "mallory",
        &mallory_token,
        AccountId(Uuid::new_v4()),
    )
    .await;
    assert!(matches!(
        receive(&mut mallory).await,
        WireFrame::Error(WireError {
            code: WireErrorCode::AuthFailed,
            retry: WireRetry::Never,
            ..
        })
    ));
    bounded("shutting down the isolated server", server.shutdown()).await;
}

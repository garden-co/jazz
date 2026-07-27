//! Jazz core WebSocket boundary.
//!
//! This route intentionally does not share the legacy `SyncPayload` `/ws`
//! transport framing.
//! It accepts postcard-encoded batches of raw `jazz::wire::WireFrame` bytes,
//! matching the workspace engine binding/server carrier shape.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    extract::State,
    extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use jazz::db::CommitUnitTrust;
use jazz::groove::records::Value as CoreValue;
use jazz::ids::AuthorId;
use jazz::protocol_limits::{MAX_WIRE_FRAME_BYTES, validate_wire_frame_len};
use jazz::wire::{
    FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireError, WireErrorCode, WireFrame,
    WireHello, WirePeerRole, WireRetry, current_wire_features, encode_frame, negotiate_wire,
};
use tokio::sync::mpsc;

use crate::public_schema::AuthMode;
use crate::server::ServerState;

const WS_REQUIRED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD;
const WS_HANDSHAKE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const WS_PER_IDENTITY_CONNECTION_CAP: usize = crate::server::PER_CLIENT_CONNECTION_CAP;
const WS_MAX_FRAME_BYTES: usize = 1 << 20;
const WS_MAX_MESSAGE_BYTES: usize = WS_MAX_FRAME_BYTES;

static WS_NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
static WS_ADMISSIONS: OnceLock<std::sync::Mutex<WebSocketAdmissionRegistry>> = OnceLock::new();

/// Jazz WebSocket endpoint.
///
/// This is a protocol boundary, not a compatibility shim for the legacy
/// `SyncPayload` websocket. The semantic `SyncMessage` loop is deliberately
/// gated on the server owning the state needed to open a real `jazz::Db`
/// peer.
pub(super) async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Response {
    if state.shutdown.is_shutting_down() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(crate::transport_error::ErrorResponse::internal(
                "server is shutting down".to_string(),
            )),
        )
            .into_response();
    }

    ws.max_frame_size(WS_MAX_FRAME_BYTES)
        .max_message_size(WS_MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_ws_connection(socket, state, headers))
}

#[derive(Clone, Debug)]
struct WebSocketAdmission {
    identity: AuthorId,
    claims: BTreeMap<String, CoreValue>,
    trust: CommitUnitTrust,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct WebSocketAdmissionKey {
    app_id: crate::AppId,
    identity: AuthorId,
}

#[derive(Debug)]
struct WebSocketAdmissionEntry {
    id: u64,
    evict_tx: mpsc::UnboundedSender<WebSocketEviction>,
}

#[derive(Debug)]
struct WebSocketEviction;

#[derive(Debug, Default)]
struct WebSocketAdmissionRegistry {
    by_key: HashMap<WebSocketAdmissionKey, VecDeque<WebSocketAdmissionEntry>>,
}

struct WebSocketAdmissionRegistration {
    key: WebSocketAdmissionKey,
    id: u64,
    evict_rx: mpsc::UnboundedReceiver<WebSocketEviction>,
}

impl Drop for WebSocketAdmissionRegistration {
    fn drop(&mut self) {
        ws_unregister_admission(self.key, self.id);
    }
}

fn ws_admission_registry() -> &'static std::sync::Mutex<WebSocketAdmissionRegistry> {
    WS_ADMISSIONS.get_or_init(Default::default)
}

fn ws_register_admission(key: WebSocketAdmissionKey) -> WebSocketAdmissionRegistration {
    let id = WS_NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel();
    let mut registry = ws_admission_registry().lock().unwrap();
    let entries = registry.by_key.entry(key).or_default();
    entries.push_back(WebSocketAdmissionEntry { id, evict_tx });

    while entries.len() > WS_PER_IDENTITY_CONNECTION_CAP {
        if let Some(oldest) = entries.pop_front() {
            let _ = oldest.evict_tx.send(WebSocketEviction);
        }
    }

    WebSocketAdmissionRegistration { key, id, evict_rx }
}

fn ws_unregister_admission(key: WebSocketAdmissionKey, id: u64) {
    let mut registry = ws_admission_registry().lock().unwrap();
    let Some(entries) = registry.by_key.get_mut(&key) else {
        return;
    };
    entries.retain(|entry| entry.id != id);
    if entries.is_empty() {
        registry.by_key.remove(&key);
    }
}

#[cfg(test)]
fn ws_live_admissions_for(key: WebSocketAdmissionKey) -> usize {
    ws_admission_registry()
        .lock()
        .unwrap()
        .by_key
        .get(&key)
        .map_or(0, VecDeque::len)
}

#[derive(serde::Deserialize)]
struct WebSocketPrelude {
    peer_identity: String,
    auth: crate::websocket_prelude_auth::AuthConfig,
}

async fn ws_admission(
    prelude: WebSocketPrelude,
    request_headers: &HeaderMap,
    state: &Arc<ServerState>,
) -> Result<WebSocketAdmission, String> {
    let peer_identity = ws_peer_identity(&prelude.peer_identity)?;
    let auth = prelude.auth;

    if let Some(admin_secret) = auth.admin_secret.as_deref() {
        crate::middleware::auth::validate_admin_secret(Some(admin_secret), &state.auth_config)
            .map_err(|(_, message)| message.to_owned())?;
        return Ok(WebSocketAdmission {
            identity: peer_identity,
            claims: BTreeMap::new(),
            trust: CommitUnitTrust::TrustedBackend,
        });
    }

    let mut headers = request_headers.clone();
    if let Some(jwt) = auth.jwt_token.as_deref() {
        let value = axum::http::HeaderValue::from_str(&format!("Bearer {jwt}"))
            .map_err(|error| format!("invalid jwt_token header value: {error}"))?;
        headers.insert(axum::http::header::AUTHORIZATION, value);
    }
    if let Some(secret) = auth.backend_secret.as_deref() {
        let value = axum::http::HeaderValue::from_str(secret)
            .map_err(|error| format!("invalid backend_secret header value: {error}"))?;
        headers.insert("X-Jazz-Backend-Secret", value);
    }
    if let Some(session_value) = auth.backend_session.as_ref() {
        use base64::Engine as _;
        let json = serde_json::to_string(session_value)
            .map_err(|error| format!("failed to serialise backend_session: {error}"))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(json.as_bytes());
        let value = axum::http::HeaderValue::from_str(&b64)
            .map_err(|error| format!("invalid backend_session header value: {error}"))?;
        headers.insert("X-Jazz-Session", value);
    }

    let has_jwt = headers.get(axum::http::header::AUTHORIZATION).is_some();
    let has_session_header = headers.get("X-Jazz-Session").is_some();
    let backend_secret = headers
        .get("X-Jazz-Backend-Secret")
        .and_then(|value| value.to_str().ok());
    if backend_secret.is_some() && !has_jwt && !has_session_header {
        crate::middleware::auth::validate_backend_secret(backend_secret, &state.auth_config)
            .map_err(|(_, message)| message.to_owned())?;
        return Ok(WebSocketAdmission {
            identity: peer_identity,
            claims: BTreeMap::new(),
            trust: CommitUnitTrust::TrustedBackend,
        });
    }

    if !has_jwt
        && !has_session_header
        && ws_has_auth_cookie(&headers, state.auth_config.auth_cookie_name.as_deref())
    {
        validate_ws_cookie_origin(&headers)?;
    }

    let session = crate::middleware::auth::extract_session(
        &headers,
        state.app_id,
        &state.auth_config,
        state.jwt_verifier.as_deref(),
    )
    .await
    .map_err(|error| {
        serde_json::to_string(&error).unwrap_or_else(|_| "authentication failed".to_owned())
    })?;

    let Some(session) = session else {
        return Err("Session required. Provide JWT, backend secret, or admin secret.".to_owned());
    };

    ws_validate_session_identity(&session.user_id, peer_identity)?;
    Ok(WebSocketAdmission {
        identity: peer_identity,
        claims: session_claims(session)?,
        trust: CommitUnitTrust::Session,
    })
}

fn session_claims(
    session: crate::public_schema::Session,
) -> Result<BTreeMap<String, CoreValue>, String> {
    let mut json = match session.claims {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    json.insert(
        "subject".to_owned(),
        serde_json::Value::String(session.user_id.clone()),
    );
    json.insert(
        "sub".to_owned(),
        serde_json::Value::String(session.user_id.clone()),
    );
    json.insert(
        "user_id".to_owned(),
        serde_json::Value::String(session.user_id),
    );
    json.insert(
        "authMode".to_owned(),
        serde_json::Value::String(
            match session.auth_mode {
                AuthMode::External => "external",
                AuthMode::LocalFirst => "local-first",
                AuthMode::Anonymous => "anonymous",
            }
            .to_owned(),
        ),
    );
    json.into_iter()
        .map(|(key, value)| json_claim_to_core_value(value).map(|value| (key, value)))
        .collect()
}

fn json_claim_to_core_value(value: serde_json::Value) -> Result<CoreValue, String> {
    match value {
        serde_json::Value::Null => Ok(CoreValue::Nullable(None)),
        serde_json::Value::Bool(value) => Ok(CoreValue::Bool(value)),
        serde_json::Value::Number(value) => value
            .as_u64()
            .map(CoreValue::U64)
            .or_else(|| value.as_f64().map(CoreValue::F64))
            .ok_or_else(|| "claims only support unsigned integers and f64 numbers".to_owned()),
        serde_json::Value::String(value) => Ok(CoreValue::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(json_claim_to_core_value)
            .collect::<Result<Vec<_>, _>>()
            .map(CoreValue::Array),
        serde_json::Value::Object(_) => {
            Err("nested claim objects are not supported yet".to_owned())
        }
    }
}

fn ws_peer_identity(identity: &str) -> Result<AuthorId, String> {
    if identity.len() != 32 {
        return Err("peer_identity must be 32 hex characters".to_owned());
    }
    let bytes: [u8; 16] = hex::decode(identity)
        .map_err(|_| "peer_identity contains non-hex digit".to_owned())?
        .try_into()
        .map_err(|_| "peer_identity must be 32 hex characters".to_owned())?;
    Ok(AuthorId::from_bytes(bytes))
}

fn ws_validate_session_identity(user_id: &str, peer_identity: AuthorId) -> Result<(), String> {
    let session_identity = uuid::Uuid::parse_str(user_id.trim())
        .map(|uuid| AuthorId::from_bytes(*uuid.as_bytes()))
        .map_err(|_| "websocket session user_id must be a UUID".to_owned())?;
    if session_identity != peer_identity {
        return Err("websocket peer_identity must match authenticated session user_id".to_owned());
    }
    Ok(())
}

fn ws_has_auth_cookie(headers: &HeaderMap, cookie_name: Option<&str>) -> bool {
    let Some(cookie_name) = cookie_name else {
        return false;
    };
    headers
        .get(axum::http::header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|cookie| {
            cookie.split(';').any(|segment| {
                let Some((name, value)) = segment.trim().split_once('=') else {
                    return false;
                };
                name == cookie_name && !value.trim().is_empty()
            })
        })
}

fn validate_ws_cookie_origin(headers: &HeaderMap) -> Result<(), String> {
    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "cookie websocket auth requires Origin header".to_owned())?;
    let host = ws_cookie_origin_host(headers)
        .ok_or_else(|| "cookie websocket auth requires Host header".to_owned())?;

    if ws_origin_matches_host(origin, host) {
        return Ok(());
    }
    Err("cookie websocket auth Origin does not match Host".to_owned())
}

fn ws_cookie_origin_host(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("X-Forwarded-Host")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            headers
                .get(axum::http::header::HOST)
                .and_then(|value| value.to_str().ok())
        })
}

fn ws_origin_matches_host(origin: &str, host: &str) -> bool {
    let Ok(origin) = reqwest::Url::parse(origin) else {
        return false;
    };
    let Some(origin_host) = origin.host_str() else {
        return false;
    };
    let origin_port = origin
        .port_or_known_default()
        .unwrap_or_else(|| match origin.scheme() {
            "https" | "wss" => 443,
            _ => 80,
        });
    let Ok(request_authority) = ws_parse_authority(host, origin_port) else {
        return false;
    };
    if origin_host.eq_ignore_ascii_case(&request_authority.host)
        && origin_port == request_authority.port
    {
        return true;
    }

    is_loopback_host(origin_host) && is_loopback_host(&request_authority.host)
}

struct WebSocketAuthority {
    host: String,
    port: u16,
}

fn ws_parse_authority(authority: &str, default_port: u16) -> Result<WebSocketAuthority, ()> {
    let parsed = reqwest::Url::parse(&format!("ws://{authority}")).map_err(|_| ())?;
    let host = parsed.host_str().ok_or(())?.to_owned();
    let port = parsed.port().unwrap_or(default_port);
    Ok(WebSocketAuthority { host, port })
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("::1")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|addr| addr.is_loopback())
}

async fn read_ws_auth_prelude(
    socket: &mut WebSocket,
    shutdown_rx: &mut tokio::sync::watch::Receiver<crate::server::ShutdownPhase>,
    state: &ServerState,
) -> Option<Vec<u8>> {
    tokio::time::timeout(WS_HANDSHAKE_READ_TIMEOUT, async {
        tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(bytes))) => Some(bytes.to_vec()),
                Some(Ok(Message::Text(text))) => Some(text.as_bytes().to_vec()),
                _ => None,
            },
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && state.shutdown.is_shutting_down() {
                    close_ws_for_shutdown(socket).await;
                }
                None
            }
        }
    })
    .await
    .unwrap_or_default()
}

async fn read_ws_frame_batch(
    socket: &mut WebSocket,
    shutdown_rx: &mut tokio::sync::watch::Receiver<crate::server::ShutdownPhase>,
    state: &ServerState,
) -> Option<Vec<u8>> {
    tokio::time::timeout(WS_HANDSHAKE_READ_TIMEOUT, async {
        tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(bytes))) => Some(bytes.to_vec()),
                _ => None,
            },
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && state.shutdown.is_shutting_down() {
                    close_ws_for_shutdown(socket).await;
                }
                None
            }
        }
    })
    .await
    .unwrap_or_default()
}

async fn handle_ws_connection(
    mut socket: WebSocket,
    state: Arc<ServerState>,
    request_headers: HeaderMap,
) {
    let mut shutdown_rx = state.shutdown.subscribe();
    let Some(_websocket_guard) = state.shutdown.try_enter_websocket() else {
        close_ws_for_shutdown(&mut socket).await;
        return;
    };

    let Some(auth_bytes) = read_ws_auth_prelude(&mut socket, &mut shutdown_rx, &state).await else {
        return;
    };
    let prelude = match serde_json::from_slice::<WebSocketPrelude>(&auth_bytes)
        .map_err(|error| format!("invalid websocket prelude: {error}"))
    {
        Ok(prelude) => prelude,
        Err(error) => {
            send_ws_error(
                &mut socket,
                WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, error),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    let admission = match ws_admission(prelude, &request_headers, &state).await {
        Ok(admission) => admission,
        Err(error) => {
            send_ws_error(
                &mut socket,
                WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, error),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    let mut admission_registration = ws_register_admission(WebSocketAdmissionKey {
        app_id: state.app_id,
        identity: admission.identity,
    });

    let Some(first) = read_ws_frame_batch(&mut socket, &mut shutdown_rx, &state).await else {
        return;
    };

    let Some(WireFrame::Hello(remote_hello)) = decode_single_ws_frame(&first).ok() else {
        send_ws_error(
            &mut socket,
            WireError::new(
                WireErrorCode::MalformedFrame,
                WireRetry::Never,
                "websocket expects first wire frame to be WireFrame::Hello",
            ),
        )
        .await;
        let _ = socket.close().await;
        return;
    };

    let negotiated = match negotiate_wire(
        &remote_hello,
        WIRE_PROTOCOL_VERSION,
        WIRE_PROTOCOL_VERSION,
        current_wire_features(),
    ) {
        Ok(negotiated) if negotiated.features & WS_REQUIRED_FEATURES != 0 => negotiated,
        Ok(_) => {
            send_ws_error(
                &mut socket,
                WireError::new(
                    WireErrorCode::UnsupportedFeature,
                    WireRetry::Never,
                    "websocket requires sync message payload frames",
                ),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
        Err(error) => {
            send_ws_error(&mut socket, error).await;
            let _ = socket.close().await;
            return;
        }
    };

    let Some(core_server_shell) = state.core_server_shell() else {
        send_ws_error(
            &mut socket,
            WireError::new(
                WireErrorCode::Internal,
                WireRetry::Never,
                "websocket requires a published schema",
            ),
        )
        .await;
        let _ = socket.close().await;
        return;
    };
    let session = match core_server_shell
        .open(admission.identity, admission.claims, admission.trust)
        .await
    {
        Ok(session) => session,
        Err(error) => {
            send_ws_error(
                &mut socket,
                WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    let server_hello =
        WireFrame::Hello(WireHello::current(WirePeerRole::Core, negotiated.features));
    let server_hello = match encode_frame(&server_hello) {
        Ok(frame) => frame,
        Err(error) => {
            send_ws_error(
                &mut socket,
                WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Never,
                    format!("failed to encode websocket server hello: {error}"),
                ),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    if send_ws_encoded_frames(&mut socket, &[server_hello])
        .await
        .is_err()
    {
        return;
    }

    tracing::info!(
        protocol_version = negotiated.protocol_version,
        features = negotiated.features,
        identity = ?admission.identity,
        "websocket negotiated"
    );

    let mut activity_rx = core_server_shell.subscribe_activity();
    if let Err(error) = drain_ws_outbound(&mut socket, &core_server_shell, session).await {
        send_ws_error(
            &mut socket,
            WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
        )
        .await;
        core_server_shell.close(session);
        let _ = socket.close().await;
        return;
    }

    loop {
        tokio::select! {
            eviction = admission_registration.evict_rx.recv() => {
                if eviction.is_some() {
                    send_ws_error(
                        &mut socket,
                        WireError::new(
                            WireErrorCode::Backpressure,
                            WireRetry::Later,
                            "websocket peer_identity connection cap exceeded",
                        ),
                    )
                    .await;
                    close_ws_for_policy(&mut socket, "websocket connection cap exceeded").await;
                }
                break;
            }
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && state.shutdown.is_shutting_down() {
                    close_ws_for_shutdown(&mut socket).await;
                    break;
                }
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(bytes))) => {
                    let frames = match decode_ws_encoded_frame_batch(&bytes) {
                        Ok(frames) => frames,
                        Err(_) => {
                            send_ws_error(
                                &mut socket,
                                WireError::new(
                                    WireErrorCode::MalformedFrame,
                                    WireRetry::Never,
                                    "failed to decode websocket frame batch",
                                ),
                            )
                            .await;
                            break;
                        }
                    };
                    let outbound = match core_server_shell.receive_tick_take(session, frames).await {
                        Ok(frames) => frames,
                        Err(error) => {
                            send_ws_error(
                                &mut socket,
                                WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
                            )
                            .await;
                            break;
                        }
                    };
                    if !outbound.is_empty()
                        && let Err(error) = send_ws_encoded_frames(&mut socket, &outbound).await {
                            send_ws_error(
                                &mut socket,
                                WireError::new(
                                    WireErrorCode::Internal,
                                    WireRetry::Later,
                                    error.to_string(),
                                ),
                            )
                            .await;
                            break;
                        }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(payload))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                _ => {}
            },
            changed = activity_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                if let Err(error) =
                    drain_ws_outbound(&mut socket, &core_server_shell, session).await
                {
                    send_ws_error(
                        &mut socket,
                        WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
                    )
                    .await;
                    break;
                }
            }
        }
    }

    core_server_shell.close(session);
    let _ = socket.close().await;
}

async fn drain_ws_outbound(
    socket: &mut WebSocket,
    core_server_shell: &crate::server::core_server_shell::ServerShellHandle,
    session: jazz_server::ServerSession,
) -> Result<(), String> {
    let outbound = core_server_shell.tick_take(session).await?;
    if outbound.is_empty() {
        return Ok(());
    }
    send_ws_encoded_frames(socket, &outbound)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn decode_single_ws_frame(bytes: &[u8]) -> Result<WireFrame, postcard::Error> {
    let mut frames = decode_ws_frame_batch(bytes)?;
    if frames.len() == 1 {
        Ok(frames.remove(0))
    } else {
        postcard::from_bytes(bytes)
    }
}

fn decode_ws_frame_batch(bytes: &[u8]) -> Result<Vec<WireFrame>, postcard::Error> {
    let encoded_frames = decode_ws_encoded_frame_batch(bytes)?;
    encoded_frames
        .iter()
        .map(|frame| jazz::wire::decode_frame(frame))
        .collect()
}

fn decode_ws_encoded_frame_batch(bytes: &[u8]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    if bytes.len() > MAX_WIRE_FRAME_BYTES {
        return Err(postcard::Error::DeserializeUnexpectedEnd);
    }
    let frames = postcard::from_bytes::<Vec<Vec<u8>>>(bytes)?;
    if frames
        .iter()
        .any(|frame| validate_wire_frame_len(frame.len()).is_err())
    {
        return Err(postcard::Error::DeserializeUnexpectedEnd);
    }
    Ok(frames)
}

async fn send_ws_encoded_frames(
    socket: &mut WebSocket,
    frames: &[Vec<u8>],
) -> Result<(), axum::Error> {
    for batch in encode_ws_frame_batches(frames).map_err(axum::Error::new)? {
        #[cfg(feature = "sync-autopsy")]
        jazz::db::sync_autopsy::record(format!(
            "server websocket send batch bytes={}",
            batch.len()
        ));
        socket.send(Message::Binary(batch)).await?;
    }
    Ok(())
}

async fn send_ws_error(socket: &mut WebSocket, error: WireError) {
    let _ = send_ws_frames(socket, &[WireFrame::Error(error)]).await;
}

async fn send_ws_frames(socket: &mut WebSocket, frames: &[WireFrame]) -> Result<(), axum::Error> {
    let encoded = frames
        .iter()
        .map(encode_frame)
        .collect::<Result<Vec<_>, _>>()
        .map_err(axum::Error::new)?;
    send_ws_encoded_frames(socket, &encoded).await
}

fn encode_ws_frame_batches(frames: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    for frame in frames {
        if validate_wire_frame_len(frame.len()).is_err() {
            return Err(postcard::Error::SerializeBufferFull);
        }
        let mut candidate = current.clone();
        candidate.push(frame.clone());
        let encoded = postcard::to_allocvec(&candidate)?;
        if encoded.len() > MAX_WIRE_FRAME_BYTES && !current.is_empty() {
            batches.push(postcard::to_allocvec(&current)?);
            current.clear();
        } else if encoded.len() > MAX_WIRE_FRAME_BYTES {
            return Err(postcard::Error::SerializeBufferFull);
        }
        current.push(frame.clone());
    }
    if !current.is_empty() {
        batches.push(postcard::to_allocvec(&current)?);
    }
    Ok(batches)
}

async fn close_ws_for_shutdown(socket: &mut WebSocket) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::RESTART,
            reason: "server shutting down".into(),
        })))
        .await;
}

async fn close_ws_for_policy(socket: &mut WebSocket, reason: &'static str) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::POLICY,
            reason: reason.into(),
        })))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    use futures::stream::FuturesUnordered;
    use futures::{SinkExt as _, StreamExt as _};
    use jazz::db::{
        Db, DbConfig, DbIdentity, PreparedQuery, QueryAttachment, ReadOpts, RowCells,
        SeededRowIdSource, WireTransportAdapter,
    };
    use jazz::groove::schema::ColumnType as CoreColumnType;
    use jazz::groove::storage::MemoryStorage as CoreMemoryStorage;
    use jazz::ids::NodeUuid;
    use jazz::query::{Query, claim, col, eq};
    use jazz::schema::{ColumnSchema, JazzSchema, Policy, TableSchema};
    use jazz::tx::DurabilityTier;
    use jazz::wire::FEATURE_STRUCTURED_ERRORS;
    use jazz::wire::decode_frame;
    use jazz::wire::{TransportError, WireTransport};
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

    use crate::AppId;
    use crate::middleware::AuthConfig;
    use crate::public_schema::Schema;
    use crate::server::core_websocket_transport::WebSocketTransport;
    use crate::server::{ServerBuilder, StorageBackend};

    const WS_STORM_SIZE: usize = 24;
    const WS_SETTLE_DEADLINE: Duration = Duration::from_secs(5);
    const WS_PUMP_DEADLINE: Duration = Duration::from_secs(5);

    #[test]
    fn ws_frame_batch_round_trips_wire_frames() {
        let frames = vec![WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            current_wire_features(),
        ))];
        let encoded = frames
            .iter()
            .map(encode_frame)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let batch = postcard::to_allocvec(&encoded).unwrap();

        assert_eq!(decode_ws_frame_batch(&batch).unwrap(), frames);
    }

    #[test]
    fn ws_peer_identity_requires_hex_author() {
        assert_eq!(
            ws_peer_identity("0102030405060708090a0b0c0d0e0f10").unwrap(),
            AuthorId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
        );

        assert!(ws_peer_identity("not-hex").is_err());
    }

    #[test]
    fn ws_session_identity_must_match_peer_identity() {
        let peer = AuthorId::from_bytes([1; 16]);
        let matching = uuid::Uuid::from_bytes([1; 16]).to_string();
        let mismatching = uuid::Uuid::from_bytes([2; 16]).to_string();

        assert!(ws_validate_session_identity(&matching, peer).is_ok());
        assert!(ws_validate_session_identity(&mismatching, peer).is_err());
        assert!(ws_validate_session_identity("not-a-uuid", peer).is_err());
    }

    #[test]
    fn ws_cookie_auth_detects_configured_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            "other=value; jazz-auth=token".parse().unwrap(),
        );

        assert!(ws_has_auth_cookie(&headers, Some("jazz-auth")));
        assert!(!ws_has_auth_cookie(&headers, Some("missing")));
        assert!(!ws_has_auth_cookie(&headers, None));
    }

    #[test]
    fn ws_cookie_origin_accepts_same_origin_and_loopback() {
        assert!(ws_origin_matches_host(
            "https://app.example:8443",
            "app.example:8443"
        ));
        assert!(ws_origin_matches_host(
            "http://localhost:5173",
            "127.0.0.1:4200"
        ));
    }

    #[test]
    fn ws_cookie_origin_uses_forwarded_host_before_host() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::ORIGIN,
            "https://app.example".parse().unwrap(),
        );
        headers.insert(axum::http::header::HOST, "internal.local".parse().unwrap());
        headers.insert("X-Forwarded-Host", "app.example".parse().unwrap());

        assert!(validate_ws_cookie_origin(&headers).is_ok());

        headers.insert("X-Forwarded-Host", "evil.example".parse().unwrap());
        assert!(validate_ws_cookie_origin(&headers).is_err());
    }

    #[test]
    fn ws_cookie_origin_uses_first_forwarded_host() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::ORIGIN,
            "https://app.example".parse().unwrap(),
        );
        headers.insert(axum::http::header::HOST, "internal.local".parse().unwrap());
        headers.insert(
            "X-Forwarded-Host",
            "app.example, proxy.local".parse().unwrap(),
        );

        assert!(validate_ws_cookie_origin(&headers).is_ok());
    }

    #[test]
    fn ws_cookie_origin_rejects_missing_or_cross_origin() {
        assert!(!ws_origin_matches_host(
            "https://evil.example",
            "app.example"
        ));

        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::HOST, "app.example".parse().unwrap());
        assert!(validate_ws_cookie_origin(&headers).is_err());

        headers.insert(
            axum::http::header::ORIGIN,
            "https://evil.example".parse().unwrap(),
        );
        assert!(validate_ws_cookie_origin(&headers).is_err());
    }

    #[test]
    fn ws_limits_are_capped_for_websocket() {
        assert_eq!(WS_MAX_FRAME_BYTES, 1 << 20);
        assert_eq!(WS_MAX_MESSAGE_BYTES, WS_MAX_FRAME_BYTES);
    }

    async fn make_ws_test_state() -> Arc<ServerState> {
        ServerBuilder::new(AppId::random())
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_owned()),
                backend_secret: Some("backend-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .with_schema(Schema::new())
            .build()
            .await
            .expect("build websocket test state")
            .state
    }

    fn ws_todos_table_schema() -> TableSchema {
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", CoreColumnType::String),
                ColumnSchema::new("done", CoreColumnType::Bool),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public())
    }

    fn ws_public_schema_convert() -> JazzSchema {
        JazzSchema::new([ws_todos_table_schema()])
    }

    fn ws_private_docs_table_schema() -> TableSchema {
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("title", CoreColumnType::String),
                ColumnSchema::new("owner", CoreColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("docs").filter(eq(col("owner"), claim("user_id"))),
        ))
        .with_write_policy(Policy::public())
    }

    fn ws_private_docs_schema_convert() -> JazzSchema {
        JazzSchema::new([ws_private_docs_table_schema()])
    }

    async fn make_ws_convergence_test_state() -> Arc<ServerState> {
        let schema = ws_public_schema_convert();
        ServerBuilder::new(AppId::random())
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_owned()),
                backend_secret: Some("backend-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .with_schema(Schema::new())
            .with_core_server_shell_schema(schema)
            .build()
            .await
            .expect("build websocket convergence test state")
            .state
    }

    #[tokio::test]
    async fn ws_backend_session_must_match_peer_identity() {
        let state = make_ws_test_state().await;
        let authenticated = AuthorId::from_bytes([0x51; 16]);
        let forged_peer = AuthorId::from_bytes([0x52; 16]);
        let prelude = WebSocketPrelude {
            peer_identity: hex::encode(forged_peer.as_bytes()),
            auth: crate::websocket_prelude_auth::AuthConfig {
                backend_secret: Some("backend-secret".to_owned()),
                backend_session: Some(serde_json::json!({
                    "user_id": uuid::Uuid::from_bytes(*authenticated.as_bytes()).to_string(),
                    "claims": {},
                    "authMode": "external",
                })),
                ..Default::default()
            },
        };

        let error = ws_admission(prelude, &HeaderMap::new(), &state)
            .await
            .expect_err("mismatched authenticated session and peer_identity must be rejected");

        assert!(
            error.contains("peer_identity must match authenticated session user_id"),
            "unexpected websocket admission error: {error}"
        );
    }

    // Internal admission-boundary test: server-shell policy reads are not yet
    // observable through a public websocket client helper, so this pins
    // the security invariant at the route admission point that feeds
    // ServerShellHandle::open(identity, claims, trust).
    #[tokio::test]
    async fn ws_backend_session_admits_session_claims_for_policy_reads() {
        let state = make_ws_test_state().await;
        let identity = AuthorId::from_bytes([0x61; 16]);
        let user_id = uuid::Uuid::from_bytes(*identity.as_bytes()).to_string();
        let prelude = WebSocketPrelude {
            peer_identity: hex::encode(identity.as_bytes()),
            auth: crate::websocket_prelude_auth::AuthConfig {
                backend_secret: Some("backend-secret".to_owned()),
                backend_session: Some(serde_json::json!({
                    "user_id": user_id,
                    "claims": {
                        "role": "reader",
                        "teams": ["eng", "ops"],
                        "beta": true,
                        "login_count": 7,
                    },
                    "authMode": "external",
                })),
                ..Default::default()
            },
        };

        let admission = ws_admission(prelude, &HeaderMap::new(), &state)
            .await
            .expect("backend session websocket admission");

        assert_eq!(admission.identity, identity);
        assert_eq!(admission.trust, CommitUnitTrust::Session);
        assert_eq!(
            admission.claims.get("role"),
            Some(&CoreValue::String("reader".to_owned()))
        );
        assert_eq!(
            admission.claims.get("teams"),
            Some(&CoreValue::Array(vec![
                CoreValue::String("eng".to_owned()),
                CoreValue::String("ops".to_owned()),
            ]))
        );
        assert_eq!(admission.claims.get("beta"), Some(&CoreValue::Bool(true)));
        assert_eq!(
            admission.claims.get("login_count"),
            Some(&CoreValue::U64(7))
        );
        assert_eq!(
            admission.claims.get("subject"),
            Some(&CoreValue::String(user_id.clone()))
        );
        assert_eq!(
            admission.claims.get("sub"),
            Some(&CoreValue::String(user_id.clone()))
        );
        assert_eq!(
            admission.claims.get("user_id"),
            Some(&CoreValue::String(user_id))
        );
        assert_eq!(
            admission.claims.get("authMode"),
            Some(&CoreValue::String("external".to_owned()))
        );
    }

    // Internal route-boundary test: this proves the reusable core
    // websocket client helper negotiates the real /apps/<APP_ID>/ws route
    // without reintroducing the legacy SyncPayload websocket handler.
    #[tokio::test]
    async fn core_websocket_transport_helper_negotiates_route_hello() {
        let state = make_ws_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;

        let transport = WebSocketTransport::connect(
            format!("http://{addr}"),
            state.app_id,
            AuthorId::from_bytes([0x41; 16]),
            crate::websocket_prelude_auth::AuthConfig {
                admin_secret: Some("admin-secret".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("websocket helper should negotiate server hello");

        let schema = ws_public_schema_convert();
        let column_families = schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let db = Db::open(
            DbConfig::new(
                schema,
                CoreMemoryStorage::new(&refs),
                DbIdentity {
                    node: NodeUuid::from_bytes([0x41; 16]),
                    author: AuthorId::from_bytes([0x41; 16]),
                },
            )
            .with_id_source(SeededRowIdSource::new(0x4100)),
        )
        .await
        .expect("open client helper client db");
        db.connect_upstream(Box::new(WireTransportAdapter::current(transport)));
        db.tick()
            .expect("client helper transport should accept db upstream frames");
    }

    async fn start_ws_test_server(state: Arc<ServerState>) -> std::net::SocketAddr {
        let app = super::super::create_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind websocket listener");
        let addr = listener.local_addr().expect("websocket listener addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve websocket test app");
        });
        addr
    }

    fn ws_url(addr: std::net::SocketAddr, app_id: AppId) -> String {
        format!("ws://{addr}/apps/{app_id}/ws")
    }

    fn ws_prelude(identity: AuthorId) -> Vec<u8> {
        format!(
            r#"{{"peer_identity":"{}","auth":{{"admin_secret":"admin-secret"}}}}"#,
            hex::encode(identity.as_bytes())
        )
        .into_bytes()
    }

    fn ws_session_prelude(identity: AuthorId) -> Vec<u8> {
        let user_id = uuid::Uuid::from_bytes(*identity.as_bytes()).to_string();
        serde_json::json!({
            "peer_identity": hex::encode(identity.as_bytes()),
            "auth": {
                "backend_secret": "backend-secret",
                "backend_session": {
                    "user_id": user_id,
                    "claims": {},
                    "authMode": "external",
                }
            }
        })
        .to_string()
        .into_bytes()
    }

    fn ws_client_hello_batch() -> Vec<u8> {
        let hello = WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS,
        ));
        let encoded = vec![encode_frame(&hello).expect("encode client hello")];
        postcard::to_allocvec(&encoded).expect("encode websocket hello batch")
    }

    #[test]
    fn websocket_frame_batches_split_near_cap_frames() {
        let frame = vec![0x42; MAX_WIRE_FRAME_BYTES - 32];
        let batches =
            encode_ws_frame_batches(&[frame.clone(), frame]).expect("encode bounded batches");

        assert_eq!(batches.len(), 2);
        for batch in batches {
            assert!(batch.len() <= MAX_WIRE_FRAME_BYTES);
            let decoded = decode_ws_encoded_frame_batch(&batch).expect("decode bounded batch");
            assert_eq!(decoded.len(), 1);
        }
    }

    #[test]
    fn websocket_frame_batches_reject_oversized_single_frame() {
        let error = encode_ws_frame_batches(&[vec![0; MAX_WIRE_FRAME_BYTES + 1]])
            .expect_err("oversized frame should not be batched");

        assert!(matches!(error, postcard::Error::SerializeBufferFull));
    }

    async fn open_negotiated_ws(
        addr: std::net::SocketAddr,
        state: &Arc<ServerState>,
        identity: AuthorId,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        open_negotiated_ws_with_prelude(addr, state, ws_prelude(identity)).await
    }

    async fn open_negotiated_ws_session(
        addr: std::net::SocketAddr,
        state: &Arc<ServerState>,
        identity: AuthorId,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        open_negotiated_ws_with_prelude(addr, state, ws_session_prelude(identity)).await
    }

    async fn open_negotiated_ws_with_prelude(
        addr: std::net::SocketAddr,
        state: &Arc<ServerState>,
        prelude: Vec<u8>,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let (mut ws, _) = connect_async(ws_url(addr, state.app_id))
            .await
            .expect("connect websocket");
        ws.send(WsMessage::Binary(prelude.into()))
            .await
            .expect("send websocket prelude");
        ws.send(WsMessage::Binary(ws_client_hello_batch().into()))
            .await
            .expect("send websocket hello");

        let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("wait for server hello")
            .expect("websocket frame")
            .expect("websocket result");
        let WsMessage::Binary(response) = response else {
            panic!("expected server hello, got {response:?}");
        };
        let frames: Vec<Vec<u8>> =
            postcard::from_bytes(&response).expect("decode websocket response batch");
        assert_eq!(frames.len(), 1);
        let WireFrame::Hello(server_hello) = decode_frame(&frames[0]).expect("decode server hello")
        else {
            panic!("expected server hello");
        };
        assert_eq!(server_hello.role, WirePeerRole::Core);
        ws
    }

    fn decode_ws_message(msg: &WsMessage) -> Vec<WireFrame> {
        let WsMessage::Binary(bytes) = msg else {
            return Vec::new();
        };
        let encoded: Vec<Vec<u8>> =
            postcard::from_bytes(bytes).expect("decode websocket frame batch");
        encoded
            .iter()
            .map(|frame| decode_frame(frame).expect("decode wire frame"))
            .collect()
    }

    #[derive(Clone, Default)]
    struct TestWireTransport {
        queues: Rc<RefCell<TestWireQueues>>,
    }

    #[derive(Default)]
    struct TestWireQueues {
        inbound: VecDeque<Vec<u8>>,
        outbound: VecDeque<Vec<u8>>,
    }

    impl TestWireTransport {
        fn push_inbound(&self, frames: impl IntoIterator<Item = Vec<u8>>) {
            self.queues.borrow_mut().inbound.extend(frames);
        }

        fn take_outbound(&self) -> Vec<Vec<u8>> {
            self.queues.borrow_mut().outbound.drain(..).collect()
        }
    }

    impl WireTransport for TestWireTransport {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            self.queues.borrow_mut().outbound.push_back(frame);
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.queues.borrow_mut().inbound.pop_front()
        }
    }

    struct TestClient {
        db: Db<CoreMemoryStorage>,
        transport: TestWireTransport,
        todos_table: TableSchema,
    }

    impl TestClient {
        async fn new(schema: JazzSchema, node_seed: u8, row_seed: u64) -> Self {
            let column_families = schema.column_families();
            let refs = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let db = Db::open(
                DbConfig::new(
                    schema,
                    CoreMemoryStorage::new(&refs),
                    DbIdentity {
                        node: NodeUuid::from_bytes([node_seed; 16]),
                        author: AuthorId::from_bytes([node_seed; 16]),
                    },
                )
                .with_id_source(SeededRowIdSource::new(row_seed)),
            )
            .await
            .expect("open client db");
            let transport = TestWireTransport::default();
            db.connect_upstream(Box::new(WireTransportAdapter::current(transport.clone())));
            Self {
                db,
                transport,
                todos_table: ws_todos_table_schema(),
            }
        }

        fn insert_todo(&self, title: &str) -> jazz::ids::RowUuid {
            self.db
                .insert(
                    "todos",
                    RowCells::from([
                        ("title".to_owned(), CoreValue::String(title.to_owned())),
                        ("done".to_owned(), CoreValue::Bool(false)),
                    ]),
                )
                .expect("insert client row")
                .row_uuid()
        }

        fn insert_private_doc(&self, title: &str, owner: AuthorId) -> jazz::ids::RowUuid {
            let owner = uuid::Uuid::from_bytes(*owner.as_bytes()).to_string();
            self.db
                .insert(
                    "docs",
                    RowCells::from([
                        ("title".to_owned(), CoreValue::String(title.to_owned())),
                        ("owner".to_owned(), CoreValue::String(owner)),
                    ]),
                )
                .expect("insert client doc")
                .row_uuid()
        }

        fn tick_take(&self) -> Vec<Vec<u8>> {
            self.db.tick().expect("tick client db");
            self.transport.take_outbound()
        }

        fn receive_tick_take(&self, frames: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
            self.transport.push_inbound(frames);
            self.tick_take()
        }

        fn attach_todos_query(&self) -> (PreparedQuery, QueryAttachment) {
            let query = self
                .db
                .prepare_query(&self.db.table("todos"))
                .expect("prepare todos query");
            let attachment = self
                .db
                .attach_query_with_opts(
                    &query,
                    ReadOpts {
                        tier: DurabilityTier::Edge,
                        ..Default::default()
                    },
                )
                .expect("default read view edge attachment should be supported");
            (query, attachment)
        }

        fn attach_table_query(&self, table: &str) -> (PreparedQuery, QueryAttachment) {
            let query = self
                .db
                .prepare_query(&self.db.table(table))
                .expect("prepare table query");
            let attachment = self
                .db
                .attach_query_with_opts(
                    &query,
                    ReadOpts {
                        tier: DurabilityTier::Edge,
                        ..Default::default()
                    },
                )
                .expect("default read view edge attachment should be supported");
            (query, attachment)
        }

        fn edge_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
            self.db.query_attachment_is_covered(attachment)
        }

        fn detach_query(&self, attachment: QueryAttachment) {
            self.db.detach_query(attachment);
        }

        async fn edge_todo_titles(&self, query: &PreparedQuery) -> Vec<String> {
            self.db
                .all(
                    query,
                    ReadOpts {
                        tier: DurabilityTier::Edge,
                        ..Default::default()
                    },
                )
                .await
                .expect("read edge todos")
                .into_iter()
                .filter_map(|row| match row.cell(&self.todos_table, "title") {
                    Some(CoreValue::String(title)) => Some(title.clone()),
                    _ => None,
                })
                .collect()
        }

        async fn edge_titles(&self, query: &PreparedQuery, table: &TableSchema) -> Vec<String> {
            self.db
                .all(
                    query,
                    ReadOpts {
                        tier: DurabilityTier::Edge,
                        ..Default::default()
                    },
                )
                .await
                .expect("read edge rows")
                .into_iter()
                .filter_map(|row| match row.cell(table, "title") {
                    Some(CoreValue::String(title)) => Some(title.clone()),
                    _ => None,
                })
                .collect()
        }
    }

    fn ws_frame_batch(frames: &[Vec<u8>]) -> Vec<u8> {
        postcard::to_allocvec(frames).expect("encode websocket frame batch")
    }

    async fn try_receive_ws_encoded_frames(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Vec<Vec<u8>> {
        let Ok(message) = tokio::time::timeout(Duration::from_millis(25), ws.next()).await else {
            return Vec::new();
        };
        let Some(Ok(WsMessage::Binary(bytes))) = message else {
            return Vec::new();
        };
        postcard::from_bytes(&bytes).unwrap_or_default()
    }

    async fn pump_core_websocket_transport_once(
        client: &TestClient,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> (usize, usize) {
        let mut outbound = client.tick_take();
        let mut sent = 0;
        let mut received = 0;
        let mut rounds = 0;
        while !outbound.is_empty() {
            rounds += 1;
            assert!(
                rounds <= 8,
                "client kept producing follow-up websocket frames"
            );
            ws.send(WsMessage::Binary(ws_frame_batch(&outbound).into()))
                .await
                .expect("send client frames");
            sent += outbound.len();
            let inbound = try_receive_ws_encoded_frames(ws).await;
            if inbound.is_empty() {
                outbound = client.tick_take();
            } else {
                received += inbound.len();
                outbound = client.receive_tick_take(inbound);
            }
        }
        (sent, received)
    }

    async fn receive_core_websocket_transport_push_once(
        client: &TestClient,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> usize {
        let inbound = try_receive_ws_encoded_frames(ws).await;
        if inbound.is_empty() {
            return 0;
        }
        let mut received = inbound.len();
        let mut outbound = client.receive_tick_take(inbound);
        let mut rounds = 0;
        while !outbound.is_empty() {
            rounds += 1;
            assert!(
                rounds <= 8,
                "client kept producing pushed follow-up websocket frames"
            );
            ws.send(WsMessage::Binary(ws_frame_batch(&outbound).into()))
                .await
                .expect("send client push follow-up frames");
            let inbound = try_receive_ws_encoded_frames(ws).await;
            if inbound.is_empty() {
                outbound = client.tick_take();
            } else {
                received += inbound.len();
                outbound = client.receive_tick_take(inbound);
            }
        }
        received
    }

    // Internal route-boundary test: until websocket has a public
    // high-level client facade, this wires two real jazz::Db clients through
    // the real /apps/<APP_ID>/ws route and proves WireFrame batches
    // flow through the server after one client writes.
    #[tokio::test(flavor = "current_thread")]
    async fn ws_clients_exchange_server_mediated_wire_frames() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let schema = ws_public_schema_convert();
        let client_a = TestClient::new(schema.clone(), 0xa1, 0xa100).await;
        let client_b = TestClient::new(schema, 0xb2, 0xb200).await;
        let mut ws_a = open_negotiated_ws(addr, &state, AuthorId::from_bytes([0xa1; 16])).await;
        let mut ws_b = open_negotiated_ws(addr, &state, AuthorId::from_bytes([0xb2; 16])).await;
        let (client_b_todos, client_b_todos_attachment) = client_b.attach_todos_query();

        let _inserted = client_a.insert_todo("route sync");

        let mut frames_sent_to_server = 0;
        let mut frames_received_from_server = 0;
        let start = tokio::time::Instant::now();
        while !client_b.edge_attachment_is_covered(&client_b_todos_attachment)
            && start.elapsed() < WS_PUMP_DEADLINE
        {
            let (sent, received) = pump_core_websocket_transport_once(&client_a, &mut ws_a).await;
            frames_sent_to_server += sent;
            frames_received_from_server += received;
            let (sent, received) = pump_core_websocket_transport_once(&client_b, &mut ws_b).await;
            frames_sent_to_server += sent;
            frames_received_from_server += received;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            frames_sent_to_server > 0,
            "the writing client must send WireFrame batches through the websocket route"
        );
        assert!(
            frames_received_from_server > 0,
            "the server must return WireFrame batches through the websocket route"
        );
        let titles = client_b.edge_todo_titles(&client_b_todos).await;
        client_b.detach_query(client_b_todos_attachment);
        assert_eq!(
            titles,
            vec!["route sync".to_owned()],
            "the receiving client must materialize the row through the websocket route"
        );
    }

    // Internal route-boundary test: this exercises the public websocket
    // route with two real jazz::Db clients. The reader registers a query and
    // receives empty coverage before the writer uploads a later row; convergence
    // must arrive through the maintained subscription path without the reader
    // re-propagating its query.
    #[tokio::test(flavor = "current_thread")]
    async fn ws_empty_covered_reader_receives_later_writer_row_without_repropagating() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let schema = ws_public_schema_convert();
        let client_b = TestClient::new(schema.clone(), 0xb2, 0xb200).await;
        let mut ws_b = open_negotiated_ws(addr, &state, AuthorId::from_bytes([0xb2; 16])).await;
        let (client_b_todos, client_b_todos_attachment) = client_b.attach_todos_query();

        let start = tokio::time::Instant::now();
        while !client_b.edge_attachment_is_covered(&client_b_todos_attachment)
            && start.elapsed() < WS_PUMP_DEADLINE
        {
            let _ = pump_core_websocket_transport_once(&client_b, &mut ws_b).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            client_b.edge_attachment_is_covered(&client_b_todos_attachment),
            "reader query must be covered by the initial empty server response"
        );
        assert!(
            client_b.edge_todo_titles(&client_b_todos).await.is_empty(),
            "reader should settle the initial covered result as empty"
        );
        client_b.detach_query(client_b_todos_attachment);

        let client_a = TestClient::new(schema, 0xa1, 0xa100).await;
        let mut ws_a = open_negotiated_ws(addr, &state, AuthorId::from_bytes([0xa1; 16])).await;
        let _inserted = client_a.insert_todo("after empty coverage");

        let start = tokio::time::Instant::now();
        let mut writer_sent = 0;
        let mut reader_received_push = 0;
        while client_b.edge_todo_titles(&client_b_todos).await.is_empty()
            && start.elapsed() < WS_PUMP_DEADLINE
        {
            let (sent, _) = pump_core_websocket_transport_once(&client_a, &mut ws_a).await;
            writer_sent += sent;
            reader_received_push +=
                receive_core_websocket_transport_push_once(&client_b, &mut ws_b).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            writer_sent > 0,
            "writer must upload the later row through the websocket route"
        );
        assert!(
            reader_received_push > 0,
            "reader must receive an unsolicited server push without re-propagating the query"
        );
        assert_eq!(
            client_b.edge_todo_titles(&client_b_todos).await,
            vec!["after empty coverage".to_owned()]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ws_reader_query_covered_empty_when_existing_row_hidden_by_read_policy() {
        let schema = ws_private_docs_schema_convert();
        let state = ServerBuilder::new(AppId::random())
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_owned()),
                backend_secret: Some("backend-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .with_schema(Schema::new())
            .with_core_server_shell_schema(schema.clone())
            .build()
            .await
            .expect("build websocket private docs test state")
            .state;
        let addr = start_ws_test_server(state.clone()).await;
        let alice = AuthorId::from_bytes([0xa1; 16]);
        let bob = AuthorId::from_bytes([0xb2; 16]);
        let client_a = TestClient::new(schema.clone(), 0xa1, 0xa100).await;
        let mut ws_a = open_negotiated_ws_session(addr, &state, alice).await;
        let _inserted = client_a.insert_private_doc("alice private", alice);

        let start = tokio::time::Instant::now();
        let mut writer_sent = 0;
        while writer_sent == 0 && start.elapsed() < WS_PUMP_DEADLINE {
            let (sent, _) = pump_core_websocket_transport_once(&client_a, &mut ws_a).await;
            writer_sent += sent;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            writer_sent > 0,
            "Alice must upload the private row through the websocket route"
        );

        let docs_table = ws_private_docs_table_schema();
        let client_b = TestClient::new(schema, 0xb2, 0xb200).await;
        let mut ws_b = open_negotiated_ws_session(addr, &state, bob).await;
        let (client_b_docs, client_b_docs_attachment) = client_b.attach_table_query("docs");

        let start = tokio::time::Instant::now();
        while !client_b.edge_attachment_is_covered(&client_b_docs_attachment)
            && start.elapsed() < WS_PUMP_DEADLINE
        {
            let _ = pump_core_websocket_transport_once(&client_b, &mut ws_b).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            client_b.edge_attachment_is_covered(&client_b_docs_attachment),
            "Bob's docs query must be covered by the websocket route"
        );
        assert!(
            client_b
                .edge_titles(&client_b_docs, &docs_table)
                .await
                .is_empty(),
            "Bob must receive empty edge rows for Alice's private row"
        );
    }

    async fn wait_for_ws_live_admissions(
        key: WebSocketAdmissionKey,
        predicate: impl Fn(usize) -> bool,
    ) -> usize {
        let start = tokio::time::Instant::now();
        let mut live = ws_live_admissions_for(key);
        while !predicate(live) && start.elapsed() < WS_SETTLE_DEADLINE {
            tokio::time::sleep(Duration::from_millis(25)).await;
            live = ws_live_admissions_for(key);
        }
        live
    }

    // Internal route-boundary test: websocket liveness is not exposed
    // through the public JazzClient API yet, so this observes the internal
    // admission registry as the user-visible socket closes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_peer_identity_connections_are_bounded_by_eviction() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let identity = AuthorId::from_bytes([0x42; 16]);
        let key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut sockets = Vec::new();
        for _ in 0..WS_PER_IDENTITY_CONNECTION_CAP {
            sockets.push(open_negotiated_ws(addr, &state, identity).await);
        }

        let mut oldest = sockets.remove(0);
        let _newest = open_negotiated_ws(addr, &state, identity).await;

        let mut saw_backpressure = false;
        let mut saw_policy_close = false;
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(msg) = oldest.next().await {
                let msg = msg.expect("oldest ws message");
                for frame in decode_ws_message(&msg) {
                    if let WireFrame::Error(error) = frame {
                        saw_backpressure = error.code == WireErrorCode::Backpressure
                            && error.retry == WireRetry::Later
                            && error.message.contains("connection cap exceeded");
                    }
                }
                if let WsMessage::Close(Some(close)) = msg {
                    saw_policy_close = close.code
                        == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy;
                    break;
                }
            }
        })
        .await
        .expect("oldest websocket should be evicted");

        assert!(
            saw_backpressure,
            "evicted websocket must receive a WireError"
        );
        assert!(
            saw_policy_close,
            "evicted websocket must receive a policy close"
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            while ws_live_admissions_for(key) > WS_PER_IDENTITY_CONNECTION_CAP {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("websocket admission cleanup");
        assert_eq!(ws_live_admissions_for(key), WS_PER_IDENTITY_CONNECTION_CAP);
    }

    // Internal route-boundary test: websocket peer admission is not
    // observable through the public JazzClient API yet, so this tests the
    // protocol boundary and its admission registry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn peer_identity_storm_is_bounded_without_rejecting_newest_connections() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let identity = AuthorId::from_bytes([0x24; 16]);
        let key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut pending = FuturesUnordered::new();
        for _ in 0..WS_STORM_SIZE {
            pending.push(open_negotiated_ws(addr, &state, identity));
        }

        let mut sockets = Vec::with_capacity(WS_STORM_SIZE);
        while let Some(ws) = pending.next().await {
            sockets.push(ws);
        }
        assert_eq!(
            sockets.len(),
            WS_STORM_SIZE,
            "websocket cap must evict older sockets, not reject new handshakes"
        );

        let live =
            wait_for_ws_live_admissions(key, |count| count <= WS_PER_IDENTITY_CONNECTION_CAP).await;
        assert!(
            live <= WS_PER_IDENTITY_CONNECTION_CAP,
            "websocket must bound live admissions per peer_identity to {WS_PER_IDENTITY_CONNECTION_CAP}; got {live}"
        );
    }

    // Internal route-boundary test: identity isolation is enforced before the
    // server shell has a higher-level public client surface to observe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn peer_identity_eviction_does_not_affect_other_identities() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let noisy_identity = AuthorId::from_bytes([0x31; 16]);
        let quiet_identity = AuthorId::from_bytes([0x32; 16]);
        let noisy_key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity: noisy_identity,
        };
        let quiet_key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity: quiet_identity,
        };

        let mut quiet_sockets = Vec::with_capacity(WS_PER_IDENTITY_CONNECTION_CAP);
        for _ in 0..WS_PER_IDENTITY_CONNECTION_CAP {
            quiet_sockets.push(open_negotiated_ws(addr, &state, quiet_identity).await);
        }
        assert_eq!(
            ws_live_admissions_for(quiet_key),
            WS_PER_IDENTITY_CONNECTION_CAP
        );

        let mut pending = FuturesUnordered::new();
        for _ in 0..WS_STORM_SIZE {
            pending.push(open_negotiated_ws(addr, &state, noisy_identity));
        }
        let mut noisy_sockets = Vec::with_capacity(WS_STORM_SIZE);
        while let Some(ws) = pending.next().await {
            noisy_sockets.push(ws);
        }

        let noisy_live =
            wait_for_ws_live_admissions(noisy_key, |count| count <= WS_PER_IDENTITY_CONNECTION_CAP)
                .await;
        assert!(
            noisy_live <= WS_PER_IDENTITY_CONNECTION_CAP,
            "noisy identity live admissions must be bounded; got {noisy_live}"
        );
        assert_eq!(
            ws_live_admissions_for(quiet_key),
            WS_PER_IDENTITY_CONNECTION_CAP,
            "quiet identity admissions must not be evicted by another peer_identity storm"
        );
        assert_eq!(quiet_sockets.len(), WS_PER_IDENTITY_CONNECTION_CAP);
        assert_eq!(noisy_sockets.len(), WS_STORM_SIZE);
    }

    // Internal route-boundary test: repeated reconnects should keep applying
    // the cap, not only the first overflow.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repeated_peer_identity_evictions_keep_live_admissions_at_cap() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let identity = AuthorId::from_bytes([0x33; 16]);
        let key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut sockets = Vec::new();
        for _ in 0..WS_PER_IDENTITY_CONNECTION_CAP {
            sockets.push(open_negotiated_ws(addr, &state, identity).await);
        }
        assert_eq!(
            wait_for_ws_live_admissions(key, |count| { count == WS_PER_IDENTITY_CONNECTION_CAP })
                .await,
            WS_PER_IDENTITY_CONNECTION_CAP
        );

        for cycle in 0..(WS_PER_IDENTITY_CONNECTION_CAP * 3) {
            sockets.push(open_negotiated_ws(addr, &state, identity).await);
            let live =
                wait_for_ws_live_admissions(key, |count| count == WS_PER_IDENTITY_CONNECTION_CAP)
                    .await;
            assert_eq!(
                live, WS_PER_IDENTITY_CONNECTION_CAP,
                "live websocket admissions must stay at cap after reconnect cycle {cycle}; got {live}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idle_ws_upgrade_is_not_held_open_indefinitely() {
        let state = make_ws_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let (mut ws, _) = connect_async(ws_url(addr, state.app_id))
            .await
            .expect("connect idle websocket");

        tokio::time::sleep(WS_HANDSHAKE_READ_TIMEOUT + Duration::from_millis(500)).await;
        let outcome = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
        assert!(
            matches!(
                outcome,
                Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None)
            ),
            "idle websocket upgrade must close after handshake timeout; observed {outcome:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idle_ws_upgrade_during_shutdown_closes_cleanly() {
        let state = make_ws_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let (mut ws, _) = connect_async(ws_url(addr, state.app_id))
            .await
            .expect("connect idle websocket");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(state.shutdown.request_shutdown());

        let outcome = tokio::time::timeout(Duration::from_secs(3), ws.next()).await;
        assert!(
            matches!(
                outcome,
                Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None)
            ),
            "idle websocket upgrade must close cleanly under shutdown; observed {outcome:?}"
        );
    }
}

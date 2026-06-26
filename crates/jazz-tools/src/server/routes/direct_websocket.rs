//! Direct jazz_core WebSocket boundary.
//!
//! This route intentionally does not share the alpha `/ws` transport framing.
//! It accepts postcard-encoded batches of raw `jazz::wire::WireFrame` bytes,
//! matching the direct core binding/server carrier shape.

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
use jazz::groove::records::Value as CoreValue;
use jazz::ids::AuthorId;
use jazz::wire::{
    FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireError,
    WireErrorCode, WireFrame, WireHello, WirePeerRole, WireRetry, encode_frame, negotiate_wire,
};
use tokio::sync::mpsc;

use crate::server::ServerState;

const DIRECT_WS_REQUIRED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD;
const DIRECT_WS_SUPPORTED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS;
const DIRECT_WS_HANDSHAKE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const DIRECT_WS_PER_IDENTITY_CONNECTION_CAP: usize = crate::server::PER_CLIENT_CONNECTION_CAP;

static DIRECT_WS_NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
static DIRECT_WS_ADMISSIONS: OnceLock<std::sync::Mutex<DirectWsAdmissionRegistry>> =
    OnceLock::new();

/// Direct jazz_core websocket endpoint.
///
/// This is a protocol boundary, not a compatibility shim for the alpha
/// `SyncPayload` websocket. The semantic `SyncMessage` loop is deliberately
/// gated on the server owning the state needed to open a real direct
/// `jazz::Db` peer.
pub(super) async fn direct_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Response {
    if state.shutdown.is_shutting_down() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(crate::jazz_transport::ErrorResponse::internal(
                "server is shutting down".to_string(),
            )),
        )
            .into_response();
    }

    ws.on_upgrade(move |socket| handle_direct_ws_connection(socket, state, headers))
}

#[derive(Clone, Debug)]
struct DirectWsAdmission {
    identity: AuthorId,
    claims: BTreeMap<String, CoreValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct DirectWsAdmissionKey {
    app_id: crate::schema_manager::AppId,
    identity: AuthorId,
}

#[derive(Debug)]
struct DirectWsAdmissionEntry {
    id: u64,
    evict_tx: mpsc::UnboundedSender<DirectWsEviction>,
}

#[derive(Debug)]
struct DirectWsEviction;

#[derive(Debug, Default)]
struct DirectWsAdmissionRegistry {
    by_key: HashMap<DirectWsAdmissionKey, VecDeque<DirectWsAdmissionEntry>>,
}

struct DirectWsAdmissionRegistration {
    key: DirectWsAdmissionKey,
    id: u64,
    evict_rx: mpsc::UnboundedReceiver<DirectWsEviction>,
}

impl Drop for DirectWsAdmissionRegistration {
    fn drop(&mut self) {
        direct_ws_unregister_admission(self.key, self.id);
    }
}

fn direct_ws_admission_registry() -> &'static std::sync::Mutex<DirectWsAdmissionRegistry> {
    DIRECT_WS_ADMISSIONS.get_or_init(Default::default)
}

fn direct_ws_register_admission(key: DirectWsAdmissionKey) -> DirectWsAdmissionRegistration {
    let id = DIRECT_WS_NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel();
    let mut registry = direct_ws_admission_registry().lock().unwrap();
    let entries = registry.by_key.entry(key).or_default();
    entries.push_back(DirectWsAdmissionEntry { id, evict_tx });

    while entries.len() > DIRECT_WS_PER_IDENTITY_CONNECTION_CAP {
        if let Some(oldest) = entries.pop_front() {
            let _ = oldest.evict_tx.send(DirectWsEviction);
        }
    }

    DirectWsAdmissionRegistration { key, id, evict_rx }
}

fn direct_ws_unregister_admission(key: DirectWsAdmissionKey, id: u64) {
    let mut registry = direct_ws_admission_registry().lock().unwrap();
    let Some(entries) = registry.by_key.get_mut(&key) else {
        return;
    };
    entries.retain(|entry| entry.id != id);
    if entries.is_empty() {
        registry.by_key.remove(&key);
    }
}

#[cfg(test)]
fn direct_ws_live_admissions_for(key: DirectWsAdmissionKey) -> usize {
    direct_ws_admission_registry()
        .lock()
        .unwrap()
        .by_key
        .get(&key)
        .map_or(0, VecDeque::len)
}

#[derive(serde::Deserialize)]
struct DirectWsPrelude {
    peer_identity: String,
    auth: crate::transport_manager::AuthConfig,
}

async fn direct_ws_admission(
    prelude: DirectWsPrelude,
    request_headers: &HeaderMap,
    state: &Arc<ServerState>,
) -> Result<DirectWsAdmission, String> {
    let peer_identity = direct_ws_peer_identity(&prelude.peer_identity)?;
    let auth = prelude.auth;

    if let Some(admin_secret) = auth.admin_secret.as_deref() {
        crate::middleware::auth::validate_admin_secret(Some(admin_secret), &state.auth_config)
            .map_err(|(_, message)| message.to_owned())?;
        return Ok(DirectWsAdmission {
            identity: peer_identity,
            claims: BTreeMap::new(),
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
        return Ok(DirectWsAdmission {
            identity: peer_identity,
            claims: BTreeMap::new(),
        });
    }

    if !has_jwt
        && !has_session_header
        && direct_ws_has_auth_cookie(&headers, state.auth_config.auth_cookie_name.as_deref())
    {
        validate_direct_ws_cookie_origin(&headers)?;
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

    direct_ws_validate_session_identity(&session.user_id, peer_identity)?;
    Ok(DirectWsAdmission {
        identity: peer_identity,
        claims: session_claims(session)?,
    })
}

fn session_claims(
    session: crate::query_manager::session::Session,
) -> Result<BTreeMap<String, CoreValue>, String> {
    let mut json = match session.claims {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    json.insert(
        "subject".to_owned(),
        serde_json::Value::String(session.user_id),
    );
    json.insert(
        "authMode".to_owned(),
        serde_json::Value::String(
            match session.auth_mode {
                crate::query_manager::session::AuthMode::External => "external",
                crate::query_manager::session::AuthMode::LocalFirst => "local-first",
                crate::query_manager::session::AuthMode::Anonymous => "anonymous",
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

fn direct_ws_peer_identity(identity: &str) -> Result<AuthorId, String> {
    if identity.len() != 32 {
        return Err("peer_identity must be 32 hex characters".to_owned());
    }
    let bytes: [u8; 16] = hex::decode(identity)
        .map_err(|_| "peer_identity contains non-hex digit".to_owned())?
        .try_into()
        .map_err(|_| "peer_identity must be 32 hex characters".to_owned())?;
    Ok(AuthorId::from_bytes(bytes))
}

fn direct_ws_validate_session_identity(
    user_id: &str,
    peer_identity: AuthorId,
) -> Result<(), String> {
    let session_identity = uuid::Uuid::parse_str(user_id.trim())
        .map(|uuid| AuthorId::from_bytes(*uuid.as_bytes()))
        .map_err(|_| "direct websocket session user_id must be a UUID".to_owned())?;
    if session_identity != peer_identity {
        return Err(
            "direct websocket peer_identity must match authenticated session user_id".to_owned(),
        );
    }
    Ok(())
}

fn direct_ws_has_auth_cookie(headers: &HeaderMap, cookie_name: Option<&str>) -> bool {
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

fn validate_direct_ws_cookie_origin(headers: &HeaderMap) -> Result<(), String> {
    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "cookie websocket auth requires Origin header".to_owned())?;
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "cookie websocket auth requires Host header".to_owned())?;

    if direct_ws_origin_matches_host(origin, host) {
        return Ok(());
    }
    Err("cookie websocket auth Origin does not match Host".to_owned())
}

fn direct_ws_origin_matches_host(origin: &str, host: &str) -> bool {
    let Ok(origin) = reqwest::Url::parse(origin) else {
        return false;
    };
    let Some(origin_host) = origin.host_str() else {
        return false;
    };
    let Ok(request_authority) = direct_ws_parse_authority(host) else {
        return false;
    };

    let origin_port = origin
        .port_or_known_default()
        .unwrap_or_else(|| match origin.scheme() {
            "https" | "wss" => 443,
            _ => 80,
        });
    if origin_host.eq_ignore_ascii_case(&request_authority.host)
        && origin_port == request_authority.port
    {
        return true;
    }

    is_loopback_host(origin_host) && is_loopback_host(&request_authority.host)
}

struct DirectWsAuthority {
    host: String,
    port: u16,
}

fn direct_ws_parse_authority(authority: &str) -> Result<DirectWsAuthority, ()> {
    let parsed = reqwest::Url::parse(&format!("ws://{authority}")).map_err(|_| ())?;
    let host = parsed.host_str().ok_or(())?.to_owned();
    let port = parsed.port_or_known_default().ok_or(())?;
    Ok(DirectWsAuthority { host, port })
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("::1")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|addr| addr.is_loopback())
}

async fn read_direct_auth_prelude(
    socket: &mut WebSocket,
    shutdown_rx: &mut tokio::sync::watch::Receiver<crate::server::ShutdownPhase>,
    state: &ServerState,
) -> Option<Vec<u8>> {
    tokio::time::timeout(DIRECT_WS_HANDSHAKE_READ_TIMEOUT, async {
        tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(bytes))) => Some(bytes.to_vec()),
                Some(Ok(Message::Text(text))) => Some(text.as_bytes().to_vec()),
                _ => None,
            },
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && state.shutdown.is_shutting_down() {
                    close_direct_ws_for_shutdown(socket).await;
                }
                None
            }
        }
    })
    .await
    .unwrap_or_default()
}

async fn read_direct_wire_frame_batch(
    socket: &mut WebSocket,
    shutdown_rx: &mut tokio::sync::watch::Receiver<crate::server::ShutdownPhase>,
    state: &ServerState,
) -> Option<Vec<u8>> {
    tokio::time::timeout(DIRECT_WS_HANDSHAKE_READ_TIMEOUT, async {
        tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(bytes))) => Some(bytes.to_vec()),
                _ => None,
            },
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && state.shutdown.is_shutting_down() {
                    close_direct_ws_for_shutdown(socket).await;
                }
                None
            }
        }
    })
    .await
    .unwrap_or_default()
}

async fn handle_direct_ws_connection(
    mut socket: WebSocket,
    state: Arc<ServerState>,
    request_headers: HeaderMap,
) {
    let mut shutdown_rx = state.shutdown.subscribe();
    let Some(_websocket_guard) = state.shutdown.try_enter_websocket() else {
        close_direct_ws_for_shutdown(&mut socket).await;
        return;
    };

    let Some(auth_bytes) = read_direct_auth_prelude(&mut socket, &mut shutdown_rx, &state).await
    else {
        return;
    };
    let prelude = match serde_json::from_slice::<DirectWsPrelude>(&auth_bytes)
        .map_err(|error| format!("invalid direct websocket prelude: {error}"))
    {
        Ok(prelude) => prelude,
        Err(error) => {
            send_direct_wire_error(
                &mut socket,
                WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, error),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    let admission = match direct_ws_admission(prelude, &request_headers, &state).await {
        Ok(admission) => admission,
        Err(error) => {
            send_direct_wire_error(
                &mut socket,
                WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, error),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    let mut admission_registration = direct_ws_register_admission(DirectWsAdmissionKey {
        app_id: state.app_id,
        identity: admission.identity,
    });

    let Some(first) = read_direct_wire_frame_batch(&mut socket, &mut shutdown_rx, &state).await
    else {
        return;
    };

    let Some(WireFrame::Hello(remote_hello)) = decode_single_direct_frame(&first).ok() else {
        send_direct_wire_error(
            &mut socket,
            WireError::new(
                WireErrorCode::MalformedFrame,
                WireRetry::Never,
                "direct websocket expects first wire frame to be WireFrame::Hello",
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
        DIRECT_WS_SUPPORTED_FEATURES,
    ) {
        Ok(negotiated) if negotiated.features & DIRECT_WS_REQUIRED_FEATURES != 0 => negotiated,
        Ok(_) => {
            send_direct_wire_error(
                &mut socket,
                WireError::new(
                    WireErrorCode::UnsupportedFeature,
                    WireRetry::Never,
                    "direct websocket requires sync message payload frames",
                ),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
        Err(error) => {
            send_direct_wire_error(&mut socket, error).await;
            let _ = socket.close().await;
            return;
        }
    };

    let Some(direct_core) = state.direct_core() else {
        send_direct_wire_error(
            &mut socket,
            WireError::new(
                WireErrorCode::Internal,
                WireRetry::Never,
                "direct websocket requires a published schema",
            ),
        )
        .await;
        let _ = socket.close().await;
        return;
    };
    let session = match direct_core.open(admission.identity, admission.claims).await {
        Ok(session) => session,
        Err(error) => {
            send_direct_wire_error(
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
            send_direct_wire_error(
                &mut socket,
                WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Never,
                    format!("failed to encode direct websocket server hello: {error}"),
                ),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    if send_direct_encoded_frames(&mut socket, &[server_hello])
        .await
        .is_err()
    {
        return;
    }

    tracing::info!(
        protocol_version = negotiated.protocol_version,
        features = negotiated.features,
        identity = ?admission.identity,
        "direct jazz_core ws negotiated"
    );

    let mut outbound_tick = tokio::time::interval(std::time::Duration::from_millis(5));
    loop {
        tokio::select! {
            eviction = admission_registration.evict_rx.recv() => {
                if eviction.is_some() {
                    send_direct_wire_error(
                        &mut socket,
                        WireError::new(
                            WireErrorCode::Backpressure,
                            WireRetry::Later,
                            "direct websocket peer_identity connection cap exceeded",
                        ),
                    )
                    .await;
                    close_direct_ws_for_policy(&mut socket, "direct websocket connection cap exceeded").await;
                }
                break;
            }
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && state.shutdown.is_shutting_down() {
                    close_direct_ws_for_shutdown(&mut socket).await;
                    break;
                }
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(bytes))) => {
                    let frames = match decode_direct_encoded_frame_batch(&bytes) {
                        Ok(frames) => frames,
                        Err(_) => {
                            send_direct_wire_error(
                                &mut socket,
                                WireError::new(
                                    WireErrorCode::MalformedFrame,
                                    WireRetry::Never,
                                    "failed to decode direct websocket frame batch",
                                ),
                            )
                            .await;
                            break;
                        }
                    };
                    let outbound = match direct_core.receive_tick_take(session, frames).await {
                        Ok(frames) => frames,
                        Err(error) => {
                            send_direct_wire_error(
                                &mut socket,
                                WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
                            )
                            .await;
                            break;
                        }
                    };
                    if !outbound.is_empty() && send_direct_encoded_frames(&mut socket, &outbound).await.is_err() {
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
            _ = outbound_tick.tick() => {
                let outbound = match direct_core.tick_take(session).await {
                    Ok(frames) => frames,
                    Err(_) => break,
                };
                if !outbound.is_empty() && send_direct_encoded_frames(&mut socket, &outbound).await.is_err() {
                    break;
                }
            }
        }
    }

    direct_core.close(session);
    let _ = socket.close().await;
}

fn decode_single_direct_frame(bytes: &[u8]) -> Result<WireFrame, postcard::Error> {
    let mut frames = decode_direct_frame_batch(bytes)?;
    if frames.len() == 1 {
        Ok(frames.remove(0))
    } else {
        postcard::from_bytes(bytes)
    }
}

fn decode_direct_frame_batch(bytes: &[u8]) -> Result<Vec<WireFrame>, postcard::Error> {
    let encoded_frames = decode_direct_encoded_frame_batch(bytes)?;
    encoded_frames
        .iter()
        .map(|frame| jazz::wire::decode_frame(frame))
        .collect()
}

fn decode_direct_encoded_frame_batch(bytes: &[u8]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    postcard::from_bytes::<Vec<Vec<u8>>>(bytes)
}

async fn send_direct_encoded_frames(
    socket: &mut WebSocket,
    frames: &[Vec<u8>],
) -> Result<(), axum::Error> {
    let batch = postcard::to_allocvec(frames).map_err(axum::Error::new)?;
    socket.send(Message::Binary(batch)).await
}

async fn send_direct_wire_error(socket: &mut WebSocket, error: WireError) {
    let _ = send_direct_wire_frames(socket, &[WireFrame::Error(error)]).await;
}

async fn send_direct_wire_frames(
    socket: &mut WebSocket,
    frames: &[WireFrame],
) -> Result<(), axum::Error> {
    let encoded = frames
        .iter()
        .map(encode_frame)
        .collect::<Result<Vec<_>, _>>()
        .map_err(axum::Error::new)?;
    let batch = postcard::to_allocvec(&encoded).map_err(axum::Error::new)?;
    socket.send(Message::Binary(batch)).await
}

async fn close_direct_ws_for_shutdown(socket: &mut WebSocket) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::RESTART,
            reason: "server shutting down".into(),
        })))
        .await;
}

async fn close_direct_ws_for_policy(socket: &mut WebSocket, reason: &'static str) {
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
    use std::time::Duration;

    use futures::stream::FuturesUnordered;
    use futures::{SinkExt as _, StreamExt as _};
    use jazz::wire::decode_frame;
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

    use crate::middleware::AuthConfig;
    use crate::query_manager::types::Schema;
    use crate::schema_manager::AppId;
    use crate::server::{ServerBuilder, StorageBackend};

    const DIRECT_WS_STORM_SIZE: usize = 24;
    const DIRECT_WS_SETTLE_DEADLINE: Duration = Duration::from_secs(5);

    #[test]
    fn direct_frame_batch_round_trips_wire_frames() {
        let frames = vec![WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            DIRECT_WS_SUPPORTED_FEATURES,
        ))];
        let encoded = frames
            .iter()
            .map(encode_frame)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let batch = postcard::to_allocvec(&encoded).unwrap();

        assert_eq!(decode_direct_frame_batch(&batch).unwrap(), frames);
    }

    #[test]
    fn direct_ws_peer_identity_requires_hex_author() {
        assert_eq!(
            direct_ws_peer_identity("0102030405060708090a0b0c0d0e0f10").unwrap(),
            AuthorId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
        );

        assert!(direct_ws_peer_identity("not-hex").is_err());
    }

    #[test]
    fn direct_ws_session_identity_must_match_peer_identity() {
        let peer = AuthorId::from_bytes([1; 16]);
        let matching = uuid::Uuid::from_bytes([1; 16]).to_string();
        let mismatching = uuid::Uuid::from_bytes([2; 16]).to_string();

        assert!(direct_ws_validate_session_identity(&matching, peer).is_ok());
        assert!(direct_ws_validate_session_identity(&mismatching, peer).is_err());
        assert!(direct_ws_validate_session_identity("not-a-uuid", peer).is_err());
    }

    #[test]
    fn direct_ws_cookie_auth_detects_configured_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            "other=value; jazz-auth=token".parse().unwrap(),
        );

        assert!(direct_ws_has_auth_cookie(&headers, Some("jazz-auth")));
        assert!(!direct_ws_has_auth_cookie(&headers, Some("missing")));
        assert!(!direct_ws_has_auth_cookie(&headers, None));
    }

    #[test]
    fn direct_ws_cookie_origin_accepts_same_origin_and_loopback() {
        assert!(direct_ws_origin_matches_host(
            "https://app.example:8443",
            "app.example:8443"
        ));
        assert!(direct_ws_origin_matches_host(
            "http://localhost:5173",
            "127.0.0.1:4200"
        ));
    }

    #[test]
    fn direct_ws_cookie_origin_rejects_missing_or_cross_origin() {
        assert!(!direct_ws_origin_matches_host(
            "https://evil.example",
            "app.example"
        ));

        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::HOST, "app.example".parse().unwrap());
        assert!(validate_direct_ws_cookie_origin(&headers).is_err());

        headers.insert(
            axum::http::header::ORIGIN,
            "https://evil.example".parse().unwrap(),
        );
        assert!(validate_direct_ws_cookie_origin(&headers).is_err());
    }

    async fn make_direct_ws_test_state() -> Arc<ServerState> {
        ServerBuilder::new(AppId::random())
            .with_auth_config(AuthConfig {
                admin_secret: Some("admin-secret".to_owned()),
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .with_schema(Schema::new())
            .build()
            .await
            .expect("build direct ws test state")
            .state
    }

    async fn start_direct_ws_test_server(state: Arc<ServerState>) -> std::net::SocketAddr {
        let app = super::super::create_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind direct ws listener");
        let addr = listener.local_addr().expect("direct ws listener addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve direct ws test app");
        });
        addr
    }

    fn direct_ws_url(addr: std::net::SocketAddr, app_id: AppId) -> String {
        format!("ws://{addr}/apps/{app_id}/ws")
    }

    fn direct_ws_prelude(identity: AuthorId) -> Vec<u8> {
        format!(
            r#"{{"peer_identity":"{}","auth":{{"admin_secret":"admin-secret"}}}}"#,
            hex::encode(identity.as_bytes())
        )
        .into_bytes()
    }

    fn direct_ws_client_hello_batch() -> Vec<u8> {
        let hello = WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS,
        ));
        let encoded = vec![encode_frame(&hello).expect("encode direct client hello")];
        postcard::to_allocvec(&encoded).expect("encode direct hello batch")
    }

    async fn open_negotiated_direct_ws(
        addr: std::net::SocketAddr,
        state: &Arc<ServerState>,
        identity: AuthorId,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let (mut ws, _) = connect_async(direct_ws_url(addr, state.app_id))
            .await
            .expect("connect direct ws");
        ws.send(WsMessage::Binary(direct_ws_prelude(identity).into()))
            .await
            .expect("send direct ws prelude");
        ws.send(WsMessage::Binary(direct_ws_client_hello_batch().into()))
            .await
            .expect("send direct ws hello");

        let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("wait for direct server hello")
            .expect("direct ws frame")
            .expect("direct ws result");
        let WsMessage::Binary(response) = response else {
            panic!("expected direct server hello, got {response:?}");
        };
        let frames: Vec<Vec<u8>> =
            postcard::from_bytes(&response).expect("decode direct ws response batch");
        assert_eq!(frames.len(), 1);
        let WireFrame::Hello(server_hello) =
            decode_frame(&frames[0]).expect("decode direct server hello")
        else {
            panic!("expected direct server hello");
        };
        assert_eq!(server_hello.role, WirePeerRole::Core);
        ws
    }

    fn decode_direct_ws_message(msg: &WsMessage) -> Vec<WireFrame> {
        let WsMessage::Binary(bytes) = msg else {
            return Vec::new();
        };
        let encoded: Vec<Vec<u8>> =
            postcard::from_bytes(bytes).expect("decode direct ws frame batch");
        encoded
            .iter()
            .map(|frame| decode_frame(frame).expect("decode direct wire frame"))
            .collect()
    }

    async fn wait_for_direct_ws_live_admissions(
        key: DirectWsAdmissionKey,
        predicate: impl Fn(usize) -> bool,
    ) -> usize {
        let start = tokio::time::Instant::now();
        let mut live = direct_ws_live_admissions_for(key);
        while !predicate(live) && start.elapsed() < DIRECT_WS_SETTLE_DEADLINE {
            tokio::time::sleep(Duration::from_millis(25)).await;
            live = direct_ws_live_admissions_for(key);
        }
        live
    }

    // Internal route-boundary test: direct websocket liveness is not exposed
    // through the public JazzClient API yet, so this observes the direct
    // admission registry as the user-visible socket closes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_direct_peer_identity_connections_are_bounded_by_eviction() {
        let state = make_direct_ws_test_state().await;
        let addr = start_direct_ws_test_server(state.clone()).await;
        let identity = AuthorId::from_bytes([0x42; 16]);
        let key = DirectWsAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut sockets = Vec::new();
        for _ in 0..DIRECT_WS_PER_IDENTITY_CONNECTION_CAP {
            sockets.push(open_negotiated_direct_ws(addr, &state, identity).await);
        }

        let mut oldest = sockets.remove(0);
        let _newest = open_negotiated_direct_ws(addr, &state, identity).await;

        let mut saw_backpressure = false;
        let mut saw_policy_close = false;
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(msg) = oldest.next().await {
                let msg = msg.expect("oldest ws message");
                for frame in decode_direct_ws_message(&msg) {
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
        .expect("oldest direct ws should be evicted");

        assert!(
            saw_backpressure,
            "evicted direct ws must receive a WireError"
        );
        assert!(
            saw_policy_close,
            "evicted direct ws must receive a policy close"
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            while direct_ws_live_admissions_for(key) > DIRECT_WS_PER_IDENTITY_CONNECTION_CAP {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("direct admission cleanup");
        assert_eq!(
            direct_ws_live_admissions_for(key),
            DIRECT_WS_PER_IDENTITY_CONNECTION_CAP
        );
    }

    // Internal route-boundary test: direct websocket peer admission is not
    // observable through the public JazzClient API yet, so this tests the
    // direct protocol boundary and its admission registry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn direct_peer_identity_storm_is_bounded_without_rejecting_newest_connections() {
        let state = make_direct_ws_test_state().await;
        let addr = start_direct_ws_test_server(state.clone()).await;
        let identity = AuthorId::from_bytes([0x24; 16]);
        let key = DirectWsAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut pending = FuturesUnordered::new();
        for _ in 0..DIRECT_WS_STORM_SIZE {
            pending.push(open_negotiated_direct_ws(addr, &state, identity));
        }

        let mut sockets = Vec::with_capacity(DIRECT_WS_STORM_SIZE);
        while let Some(ws) = pending.next().await {
            sockets.push(ws);
        }
        assert_eq!(
            sockets.len(),
            DIRECT_WS_STORM_SIZE,
            "direct ws cap must evict older sockets, not reject new handshakes"
        );

        let live = wait_for_direct_ws_live_admissions(key, |count| {
            count <= DIRECT_WS_PER_IDENTITY_CONNECTION_CAP
        })
        .await;
        assert!(
            live <= DIRECT_WS_PER_IDENTITY_CONNECTION_CAP,
            "direct ws must bound live admissions per peer_identity to {DIRECT_WS_PER_IDENTITY_CONNECTION_CAP}; got {live}"
        );
    }

    // Internal route-boundary test: identity isolation is enforced before the
    // direct core has a higher-level public client surface to observe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn direct_peer_identity_eviction_does_not_affect_other_identities() {
        let state = make_direct_ws_test_state().await;
        let addr = start_direct_ws_test_server(state.clone()).await;
        let noisy_identity = AuthorId::from_bytes([0x31; 16]);
        let quiet_identity = AuthorId::from_bytes([0x32; 16]);
        let noisy_key = DirectWsAdmissionKey {
            app_id: state.app_id,
            identity: noisy_identity,
        };
        let quiet_key = DirectWsAdmissionKey {
            app_id: state.app_id,
            identity: quiet_identity,
        };

        let mut quiet_sockets = Vec::with_capacity(DIRECT_WS_PER_IDENTITY_CONNECTION_CAP);
        for _ in 0..DIRECT_WS_PER_IDENTITY_CONNECTION_CAP {
            quiet_sockets.push(open_negotiated_direct_ws(addr, &state, quiet_identity).await);
        }
        assert_eq!(
            direct_ws_live_admissions_for(quiet_key),
            DIRECT_WS_PER_IDENTITY_CONNECTION_CAP
        );

        let mut pending = FuturesUnordered::new();
        for _ in 0..DIRECT_WS_STORM_SIZE {
            pending.push(open_negotiated_direct_ws(addr, &state, noisy_identity));
        }
        let mut noisy_sockets = Vec::with_capacity(DIRECT_WS_STORM_SIZE);
        while let Some(ws) = pending.next().await {
            noisy_sockets.push(ws);
        }

        let noisy_live = wait_for_direct_ws_live_admissions(noisy_key, |count| {
            count <= DIRECT_WS_PER_IDENTITY_CONNECTION_CAP
        })
        .await;
        assert!(
            noisy_live <= DIRECT_WS_PER_IDENTITY_CONNECTION_CAP,
            "noisy identity live admissions must be bounded; got {noisy_live}"
        );
        assert_eq!(
            direct_ws_live_admissions_for(quiet_key),
            DIRECT_WS_PER_IDENTITY_CONNECTION_CAP,
            "quiet identity admissions must not be evicted by another peer_identity storm"
        );
        assert_eq!(quiet_sockets.len(), DIRECT_WS_PER_IDENTITY_CONNECTION_CAP);
        assert_eq!(noisy_sockets.len(), DIRECT_WS_STORM_SIZE);
    }

    // Internal route-boundary test: repeated reconnects should keep applying
    // the cap, not only the first overflow.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repeated_direct_peer_identity_evictions_keep_live_admissions_at_cap() {
        let state = make_direct_ws_test_state().await;
        let addr = start_direct_ws_test_server(state.clone()).await;
        let identity = AuthorId::from_bytes([0x33; 16]);
        let key = DirectWsAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut sockets = Vec::new();
        for _ in 0..DIRECT_WS_PER_IDENTITY_CONNECTION_CAP {
            sockets.push(open_negotiated_direct_ws(addr, &state, identity).await);
        }
        assert_eq!(
            wait_for_direct_ws_live_admissions(key, |count| {
                count == DIRECT_WS_PER_IDENTITY_CONNECTION_CAP
            })
            .await,
            DIRECT_WS_PER_IDENTITY_CONNECTION_CAP
        );

        for cycle in 0..(DIRECT_WS_PER_IDENTITY_CONNECTION_CAP * 3) {
            sockets.push(open_negotiated_direct_ws(addr, &state, identity).await);
            let live = wait_for_direct_ws_live_admissions(key, |count| {
                count == DIRECT_WS_PER_IDENTITY_CONNECTION_CAP
            })
            .await;
            assert_eq!(
                live, DIRECT_WS_PER_IDENTITY_CONNECTION_CAP,
                "live direct admissions must stay at cap after reconnect cycle {cycle}; got {live}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idle_direct_ws_upgrade_is_not_held_open_indefinitely() {
        let state = make_direct_ws_test_state().await;
        let addr = start_direct_ws_test_server(state.clone()).await;
        let (mut ws, _) = connect_async(direct_ws_url(addr, state.app_id))
            .await
            .expect("connect idle direct ws");

        tokio::time::sleep(DIRECT_WS_HANDSHAKE_READ_TIMEOUT + Duration::from_millis(500)).await;
        let outcome = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
        assert!(
            matches!(
                outcome,
                Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None)
            ),
            "idle direct ws upgrade must close after handshake timeout; observed {outcome:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idle_direct_ws_upgrade_during_shutdown_closes_cleanly() {
        let state = make_direct_ws_test_state().await;
        let addr = start_direct_ws_test_server(state.clone()).await;
        let (mut ws, _) = connect_async(direct_ws_url(addr, state.app_id))
            .await
            .expect("connect idle direct ws");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(state.shutdown.request_shutdown());

        let outcome = tokio::time::timeout(Duration::from_secs(3), ws.next()).await;
        assert!(
            matches!(
                outcome,
                Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None)
            ),
            "idle direct ws upgrade must close cleanly under shutdown; observed {outcome:?}"
        );
    }
}

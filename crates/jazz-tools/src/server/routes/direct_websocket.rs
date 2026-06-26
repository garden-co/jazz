//! Direct jazz_core WebSocket boundary.
//!
//! This route intentionally does not share the alpha `/ws` transport framing.
//! It accepts postcard-encoded batches of raw `jazz::wire::WireFrame` bytes,
//! matching the direct core binding/server carrier shape.

use std::collections::BTreeMap;
use std::sync::Arc;

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

use crate::server::ServerState;

const DIRECT_WS_REQUIRED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD;
const DIRECT_WS_SUPPORTED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS;
const DIRECT_WS_HANDSHAKE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

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
    match tokio::time::timeout(DIRECT_WS_HANDSHAKE_READ_TIMEOUT, async {
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
    {
        Ok(bytes) => bytes,
        Err(_) => None,
    }
}

async fn read_direct_wire_frame_batch(
    socket: &mut WebSocket,
    shutdown_rx: &mut tokio::sync::watch::Receiver<crate::server::ShutdownPhase>,
    state: &ServerState,
) -> Option<Vec<u8>> {
    match tokio::time::timeout(DIRECT_WS_HANDSHAKE_READ_TIMEOUT, async {
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
    {
        Ok(bytes) => bytes,
        Err(_) => None,
    }
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

    let Some(direct_core) = state.direct_core.clone() else {
        send_direct_wire_error(
            &mut socket,
            WireError::new(
                WireErrorCode::Internal,
                WireRetry::Never,
                "direct websocket requires a fixed schema server",
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

#[cfg(test)]
mod tests {
    use super::*;

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
}

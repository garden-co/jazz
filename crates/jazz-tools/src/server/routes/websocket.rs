//! WebSocket handler — handshake authentication, connection lifecycle, and cleanup.

//! HTTP routes for the Jazz server.

use std::sync::Arc;

use axum::{
    extract::State,
    extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    http::HeaderMap,
    response::{IntoResponse, Response},
};

use crate::middleware::auth::{extract_session, validate_admin_secret, validate_backend_secret};
use crate::server::{ConnectionState, ServerState};
use crate::sync_manager::ClientId;

use super::utils::connection_schema_diagnostics_from_handshake;

const MAX_WS_SYNC_UPDATES_PER_FRAME: usize = 256;

/// Generous ceiling on the decompressed size of the pre-auth handshake frame.
/// An `AuthHandshake` is a few KB of JSON, so this only rejects an obvious LZ4
/// decompression bomb sent by an unauthenticated peer before any auth runs.
const MAX_HANDSHAKE_DECOMPRESSED_BYTES: usize = 1024 * 1024;

/// Maximum time the server waits for a client to send its `AuthHandshake`
/// frame after the WS upgrade completes. Closes the slowloris pattern
/// where an attacker pins server-side state by opening upgrades and never
/// sending the first frame. See jaz0-a803.
pub(crate) const HANDSHAKE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Maximum size of an inbound WebSocket message (the compressed bytes on the
/// wire). This is axum's existing default, set explicitly so the limit is
/// visible and can later be made configurable. It is *not* a bound on the
/// decompressed payload — see `MAX_HANDSHAKE_DECOMPRESSED_BYTES` and the
/// follow-up on bounded framing for that.
const MAX_WS_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

pub(super) async fn ws_handler(
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

    ws.max_message_size(MAX_WS_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_ws_connection(socket, state, headers))
}

/// Outcome of authenticating a WS handshake.
#[derive(Debug)]
pub(super) enum WsClientSetup {
    Backend,
    Session(crate::query_manager::session::Session),
}

/// Authenticate a WebSocket `AuthHandshake`.
///
/// Priority is:
/// 1. `admin_secret` valid → `WsClientSetup::Backend`
/// 2. `backend_secret` present + no session header → `WsClientSetup::Backend`
/// 3. Otherwise → `extract_session` → `WsClientSetup::Session`
///
/// Returns `Err(message)` on auth failure; the caller should send a
/// `ServerEvent::Error` frame before closing.
pub(super) async fn authenticate_ws_handshake(
    handshake: &crate::transport_manager::AuthHandshake,
    request_headers: &HeaderMap,
    state: &Arc<ServerState>,
) -> Result<WsClientSetup, String> {
    use axum::http::HeaderValue;
    use base64::Engine as _;

    let auth = &handshake.auth;

    // `admin_secret` is an explicit request to run this WS transport as the
    // backend. Validate it first and short-circuit all user-scoped auth.
    if let Some(admin_secret) = auth.admin_secret.as_deref() {
        validate_admin_secret(Some(admin_secret), &state.auth_config)
            .map_err(|(_, msg)| msg.to_string())?;
        return Ok(WsClientSetup::Backend);
    }

    if request_uses_cookie_auth(handshake, request_headers, &state.auth_config) {
        validate_ws_cookie_origin(request_headers)?;
    }

    // Build a synthetic HeaderMap from the handshake auth fields, layered on
    // top of the original upgrade request so cookie-based auth remains visible.
    let mut headers = request_headers.clone();

    if let Some(jwt) = &auth.jwt_token {
        let value = HeaderValue::from_str(&format!("Bearer {jwt}"))
            .map_err(|e| format!("invalid jwt_token header value: {e}"))?;
        headers.insert(axum::http::header::AUTHORIZATION, value);
    }
    if let Some(secret) = &auth.backend_secret {
        let value = HeaderValue::from_str(secret)
            .map_err(|e| format!("invalid backend_secret header value: {e}"))?;
        headers.insert("X-Jazz-Backend-Secret", value);
    }
    if let Some(session_val) = &auth.backend_session {
        let json = serde_json::to_string(session_val)
            .map_err(|e| format!("failed to serialise backend_session: {e}"))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(json.as_bytes());
        let value = HeaderValue::from_str(&b64)
            .map_err(|e| format!("invalid backend_session header value: {e}"))?;
        headers.insert("X-Jazz-Session", value);
    }

    let has_jwt = headers.get(axum::http::header::AUTHORIZATION).is_some();
    let has_session_header = headers.get("X-Jazz-Session").is_some();
    let backend_secret = headers
        .get("X-Jazz-Backend-Secret")
        .and_then(|v| v.to_str().ok());

    // 2. Backend secret — only when no user-scoped JWT is present.  Clients
    //    that carry both a backend_secret and a jwt_token (e.g. test helpers
    //    that mirror the full credential set) must be treated as users so the
    //    connection carries a session for row-level policy evaluation.
    if backend_secret.is_some() && !has_jwt && !has_session_header {
        validate_backend_secret(backend_secret, &state.auth_config)
            .map_err(|(_, msg)| msg.to_string())?;
        return Ok(WsClientSetup::Backend);
    }

    // 3. JWT / session-impersonation path.
    let session = extract_session(
        &headers,
        state.app_id,
        &state.auth_config,
        state.jwt_verifier.as_deref(),
    )
    .await
    .map_err(|e| serde_json::to_string(&e).unwrap_or_else(|_| "authentication failed".into()))?;

    let session =
        session.ok_or_else(|| "Session required. Provide JWT or backend secret.".to_string())?;

    Ok(WsClientSetup::Session(session))
}

fn request_uses_cookie_auth(
    handshake: &crate::transport_manager::AuthHandshake,
    request_headers: &HeaderMap,
    auth_config: &crate::middleware::AuthConfig,
) -> bool {
    let Some(cookie_name) = auth_config.auth_cookie_name.as_deref() else {
        return false;
    };

    let has_explicit_auth = handshake.auth.jwt_token.is_some()
        || handshake.auth.backend_secret.is_some()
        || handshake.auth.backend_session.is_some()
        || handshake.auth.admin_secret.is_some()
        || request_headers
            .get(axum::http::header::AUTHORIZATION)
            .is_some()
        || request_headers.get("X-Jazz-Backend-Secret").is_some()
        || request_headers.get("X-Jazz-Session").is_some()
        || request_headers.get("X-Jazz-Admin-Secret").is_some();

    if has_explicit_auth {
        return false;
    }

    request_cookie_value(request_headers, cookie_name).is_some()
}

fn request_cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let cookie_header = headers
        .get(axum::http::header::COOKIE)
        .and_then(|value| value.to_str().ok())?;

    cookie_header.split(';').find_map(|segment| {
        let trimmed = segment.trim();
        let (candidate_name, candidate_value) = trimmed.split_once('=')?;
        if candidate_name == name && !candidate_value.is_empty() {
            Some(candidate_value)
        } else {
            None
        }
    })
}

fn validate_ws_cookie_origin(headers: &HeaderMap) -> Result<(), String> {
    let host = headers
        .get("X-Forwarded-Host")
        .or_else(|| headers.get(axum::http::header::HOST))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Cookie auth requires Host header".to_string())?;

    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Cookie auth requires Origin header".to_string())?;

    let origin_uri: axum::http::Uri = origin
        .parse()
        .map_err(|_| "Cookie auth requires a valid Origin header".to_string())?;
    let origin_authority = origin_uri
        .authority()
        .map(|authority| authority.as_str())
        .ok_or_else(|| "Cookie auth requires an Origin authority".to_string())?;

    let is_allowed_origin = origin_authority.eq_ignore_ascii_case(host)
        || is_loopback_cookie_origin(origin_uri.scheme_str(), origin_authority, host)?;

    if is_allowed_origin {
        Ok(())
    } else {
        Err("Cookie auth Origin must match Host".to_string())
    }
}

fn is_loopback_cookie_origin(
    origin_scheme: Option<&str>,
    origin_authority: &str,
    host: &str,
) -> Result<bool, String> {
    if !matches!(origin_scheme, Some("http") | Some("https")) {
        return Ok(false);
    }

    let origin_authority: axum::http::uri::Authority = origin_authority
        .parse()
        .map_err(|_| "Cookie auth requires a valid Origin authority".to_string())?;
    let host_authority: axum::http::uri::Authority = host
        .parse()
        .map_err(|_| "Cookie auth requires a valid Host header".to_string())?;

    Ok(
        is_loopback_dev_host(origin_authority.host())
            && is_loopback_dev_host(host_authority.host()),
    )
}

fn is_loopback_dev_host(host: &str) -> bool {
    let host = host.trim_matches(['[', ']']);
    host.eq_ignore_ascii_case("localhost")
        || host.to_ascii_lowercase().ends_with(".localhost")
        || host == "127.0.0.1"
        || host == "::1"
}

/// Send a `ServerEvent::Error` frame on the socket, best-effort.
async fn send_ws_error(socket: &mut WebSocket, message: &str) {
    send_ws_error_with_code(
        socket,
        crate::jazz_transport::ErrorCode::Unauthorized,
        message,
    )
    .await;
}

/// Send a `ServerEvent::Error` frame on the socket, best-effort.
///
/// Uses JSON encoding so a not-yet-authenticated peer (which doesn't know
/// the post-handshake binary wire format yet) can still decode the error.
async fn send_ws_error_with_code(
    socket: &mut WebSocket,
    code: crate::jazz_transport::ErrorCode,
    message: &str,
) {
    let event = crate::jazz_transport::ServerEvent::Error {
        message: message.to_string(),
        code,
    };
    if let Ok(bytes) = serde_json::to_vec(&event) {
        let frame = crate::transport_manager::frame_encode(&bytes);
        let _ = socket.send(Message::Binary(frame)).await;
    }
}

/// Send a `ServerEvent::Error` frame using the post-handshake binary
/// wire format. Use this from any path that runs after the
/// `ConnectedResponse` has been sent — clients post-handshake parse
/// frames via `ServerEvent::decode_payload`, not JSON.
async fn send_ws_error_binary(
    socket: &mut WebSocket,
    code: crate::jazz_transport::ErrorCode,
    message: &str,
) {
    let event = crate::jazz_transport::ServerEvent::Error {
        message: message.to_string(),
        code,
    };
    if let Ok(bytes) = event.encode_payload() {
        let frame = crate::transport_manager::frame_encode(&bytes);
        let _ = socket.send(Message::Binary(frame)).await;
    }
}

async fn close_ws_with_protocol_reason(socket: &mut WebSocket, reason: &str) {
    let reason = reason.chars().take(123).collect::<String>();
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::PROTOCOL,
            reason: reason.into(),
        })))
        .await;
}

async fn close_ws_with_policy_reason(socket: &mut WebSocket, reason: &str) {
    let reason = reason.chars().take(123).collect::<String>();
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::POLICY,
            reason: reason.into(),
        })))
        .await;
}

async fn close_ws_for_shutdown(socket: &mut WebSocket) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::RESTART,
            reason: "server shutting down".into(),
        })))
        .await;
}

/// Decide the edge-fallback redirect target for a freshly-accepted connection:
/// `Some(url)` when the connection was forwarded here (the landing gateway set
/// `x-jazz-forwarded`) **and** a public URL is configured to advertise; `None`
/// otherwise (serve normally). Pure so the gating is unit-testable.
fn forwarded_redirect_target(public_url: Option<&str>, headers: &HeaderMap) -> Option<String> {
    let public_url = public_url?;
    headers
        .contains_key("x-jazz-forwarded")
        .then(|| public_url.to_string())
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
    if state.shutdown.is_shutting_down() {
        close_ws_for_shutdown(&mut socket).await;
        return;
    }

    // 1. Read the first binary frame — expected to be AuthHandshake.
    //    Bounded read so unauthenticated peers can't pin server-side
    //    resources by opening upgrades without sending a handshake.
    let first = tokio::select! {
        msg = socket.recv() => match msg {
            Some(Ok(Message::Binary(b))) => b,
            _ => {
                let _ = socket.close().await;
                return;
            }
        },
        changed = shutdown_rx.changed() => {
            if changed.is_ok() && state.shutdown.is_shutting_down() {
                close_ws_for_shutdown(&mut socket).await;
            } else {
                let _ = socket.close().await;
            }
            return;
        }
        _ = tokio::time::sleep(HANDSHAKE_READ_TIMEOUT) => {
            close_ws_with_policy_reason(&mut socket, "handshake timeout").await;
            return;
        }
    };
    let payload = match crate::transport_manager::frame_decode_capped(
        &first,
        MAX_HANDSHAKE_DECOMPRESSED_BYTES,
    ) {
        Some(payload) => payload,
        None => {
            let _ = socket.close().await;
            return;
        }
    };
    let handshake =
        match serde_json::from_slice::<crate::transport_manager::AuthHandshake>(&payload) {
            Ok(h) => h,
            Err(_) => {
                let _ = socket.close().await;
                return;
            }
        };

    // Older, pre-versioned clients deserialize as protocol version 0. Reject
    // them explicitly so developers see an actionable update prompt instead
    // of a dropped socket.
    if handshake.sync_protocol_version != crate::transport_manager::SYNC_PROTOCOL_VERSION {
        let message = format!(
            "Incompatible Jazz sync protocol: client sent {}, server requires {}. Please update Jazz.",
            handshake.sync_protocol_version,
            crate::transport_manager::SYNC_PROTOCOL_VERSION,
        );
        // Use BadRequest here so older clients that do not know newer error
        // codes can still deserialize and log the message.
        send_ws_error_with_code(
            &mut socket,
            crate::jazz_transport::ErrorCode::BadRequest,
            &message,
        )
        .await;
        close_ws_with_protocol_reason(&mut socket, &message).await;
        return;
    }

    // 2. Parse client_id.
    let client_id = match crate::sync_manager::ClientId::parse(&handshake.client_id) {
        Some(id) => id,
        None => {
            send_ws_error(&mut socket, "missing or invalid client_id").await;
            let _ = socket.close().await;
            return;
        }
    };

    // 3. Authenticate.
    let setup = tokio::select! {
        auth = authenticate_ws_handshake(&handshake, &request_headers, &state) => match auth {
            Ok(s) => s,
            Err(msg) => {
                send_ws_error(&mut socket, &msg).await;
                let _ = socket.close().await;
                return;
            }
        },
        changed = shutdown_rx.changed() => {
            if changed.is_ok() && state.shutdown.is_shutting_down() {
                close_ws_for_shutdown(&mut socket).await;
            } else {
                let _ = socket.close().await;
            }
            return;
        }
    };
    let role = match &setup {
        WsClientSetup::Backend => "backend",
        WsClientSetup::Session(_) => "session",
    };

    // 4. Register with ConnectionEventHub (mirrors events_handler).
    let connection_id = state
        .next_connection_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let crate::server::ConnectionRegistration {
        next_sync_seq,
        receiver: mut sync_rx,
        evicted: evicted_flag,
    } = state
        .connection_event_hub
        .register_connection(connection_id, client_id);
    {
        let mut connections = state.connections.write().await;
        connections.insert(connection_id, ConnectionState { client_id });
    }
    state.on_client_connected(client_id).await;

    // 5. Ensure the client state in the runtime.
    match setup {
        WsClientSetup::Backend => {
            let _ = state
                .runtime
                .ensure_client_as_backend_with_catalogue_state_hash(
                    client_id,
                    handshake.catalogue_state_hash.as_deref(),
                );
        }
        WsClientSetup::Session(session) => {
            let _ = state
                .runtime
                .ensure_client_with_session_and_catalogue_state_hash(
                    client_id,
                    session,
                    handshake.catalogue_state_hash.as_deref(),
                );
        }
    }

    // 5b. Dispatch connection schema diagnostics if client sent a declared schema hash.
    match connection_schema_diagnostics_from_handshake(&state, &handshake) {
        Ok(Some(diagnostics)) => {
            state.connection_event_hub.dispatch_payload(
                client_id,
                crate::sync_manager::SyncPayload::ConnectionSchemaDiagnostics(diagnostics),
            );
        }
        Ok(None) => {}
        Err(err) => {
            tracing::error!(
                %client_id,
                declared_schema_hash = ?handshake.declared_schema_hash,
                "failed to compute connection schema diagnostics: {err}"
            );
        }
    }

    // 6. Send the Connected response.
    let resp = crate::transport_manager::ConnectedResponse {
        sync_protocol_version: crate::transport_manager::SYNC_PROTOCOL_VERSION,
        connection_id: connection_id.to_string(),
        client_id: client_id.to_string(),
        next_sync_seq: Some(next_sync_seq),
        catalogue_state_hash: state.runtime.catalogue_state_hash().ok(),
    };
    let resp_bytes = match serde_json::to_vec(&resp) {
        Ok(b) => b,
        Err(_) => {
            ws_cleanup(&state, connection_id, client_id).await;
            let _ = socket.close().await;
            return;
        }
    };
    let connected_frame = crate::transport_manager::frame_encode(&resp_bytes);
    if socket.send(Message::Binary(connected_frame)).await.is_err() {
        ws_cleanup(&state, connection_id, client_id).await;
        return;
    }
    tracing::info!(connection_id, %client_id, role, "ws client connected");

    // 6b. Edge-fallback redirect. If this connection was proxied here from a
    //     region where the tenant isn't present — the landing gateway stamps
    //     `x-jazz-forwarded` — and we have a public URL to advertise, offer the
    //     client a direct reconnect URL in-band and keep serving. The client
    //     migrates at its own pace and closes its own old socket; older clients
    //     that don't recognize the frame ignore it and stay on the proxy. The
    //     URL is pure deployment config (JAZZ_PUBLIC_URL) — no topology here.
    if let Some(url) = forwarded_redirect_target(state.public_url.as_deref(), &request_headers) {
        let event = crate::jazz_transport::ServerEvent::Redirect { url: url.clone() };
        if let Ok(bytes) = event.encode_payload() {
            let frame = crate::transport_manager::frame_encode(&bytes);
            if socket.send(Message::Binary(frame)).await.is_err() {
                ws_cleanup(&state, connection_id, client_id).await;
                return;
            }
            tracing::info!(connection_id, %client_id, public_url = %url, "offered edge-fallback redirect");
        }
    }

    // 7. Bidirectional loop: inbound frames from client + outbound updates from hub.
    //    Also fires a periodic heartbeat so idle connections don't look half-open.
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    // Don't emit a heartbeat immediately after Connected — wait a full tick.
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && state.shutdown.is_shutting_down() {
                    close_ws_for_shutdown(&mut socket).await;
                    break;
                }
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(data))) => {
                    let Some(payload) = crate::transport_manager::frame_decode(&data) else {
                        continue;
                    };
                    if let Err(e) = state.process_ws_client_frame(client_id, &payload).await {
                        tracing::warn!(error = ?e, "ws client frame rejected");
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                _ => continue,
            },
            update = sync_rx.recv() => {
                let Some(u) = update else {
                    // Distinguish per-client-cap eviction from a normal
                    // disconnect, so the evicted client gets a programmatic
                    // signal instead of an unexplained TCP close.
                    if evicted_flag.load(std::sync::atomic::Ordering::SeqCst) {
                        send_ws_error_binary(
                            &mut socket,
                            crate::jazz_transport::ErrorCode::RateLimited,
                            "per-client connection cap exceeded",
                        )
                        .await;
                        close_ws_with_policy_reason(
                            &mut socket,
                            "per-client connection cap exceeded",
                        )
                        .await;
                    }
                    break;
                };
                let mut updates = Vec::with_capacity(MAX_WS_SYNC_UPDATES_PER_FRAME);
                updates.push(crate::jazz_transport::SequencedSyncPayload {
                    seq: Some(u.seq),
                    payload: u.payload,
                });
                while updates.len() < MAX_WS_SYNC_UPDATES_PER_FRAME {
                    match sync_rx.try_recv() {
                        Ok(u) => updates.push(crate::jazz_transport::SequencedSyncPayload {
                            seq: Some(u.seq),
                            payload: u.payload,
                        }),
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
                let event = if updates.len() == 1 {
                    let update = updates.pop().expect("single update is present");
                    crate::jazz_transport::ServerEvent::SyncUpdate {
                        seq: update.seq,
                        payload: Box::new(update.payload),
                    }
                } else {
                    crate::jazz_transport::ServerEvent::SyncUpdateBatch { updates }
                };
                let bytes = match event.encode_payload() {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let frame = crate::transport_manager::frame_encode(&bytes);
                if socket.send(Message::Binary(frame)).await.is_err() {
                    break;
                }
            }
            _ = heartbeat.tick() => {
                let event = crate::jazz_transport::ServerEvent::Heartbeat;
                let Ok(bytes) = event.encode_payload() else { continue };
                let frame = crate::transport_manager::frame_encode(&bytes);
                if socket.send(Message::Binary(frame)).await.is_err() {
                    break;
                }
            }
        }
    }

    ws_cleanup(&state, connection_id, client_id).await;
    let _ = socket.close().await;
}

/// Disconnect cleanup: mirrors the drop path in `events_handler`.
async fn ws_cleanup(state: &Arc<ServerState>, connection_id: u64, client_id: ClientId) {
    {
        let mut connections = state.connections.write().await;
        connections.remove(&connection_id);
    }
    state
        .connection_event_hub
        .unregister_connection(connection_id);
    state.on_connection_closed(client_id).await;
}

#[cfg(test)]
mod redirect_tests {
    use super::forwarded_redirect_target;
    use axum::http::HeaderMap;

    fn headers_with(forwarded: bool) -> HeaderMap {
        let mut h = HeaderMap::new();
        if forwarded {
            h.insert("x-jazz-forwarded", "1".parse().unwrap());
        }
        h
    }

    #[test]
    fn redirects_when_forwarded_and_public_url_set() {
        assert_eq!(
            forwarded_redirect_target(Some("https://us-east-2.example"), &headers_with(true))
                .as_deref(),
            Some("https://us-east-2.example"),
        );
    }

    #[test]
    fn no_redirect_without_forwarded_marker() {
        assert!(
            forwarded_redirect_target(Some("https://us-east-2.example"), &headers_with(false))
                .is_none()
        );
    }

    #[test]
    fn no_redirect_without_public_url() {
        assert!(forwarded_redirect_target(None, &headers_with(true)).is_none());
    }
}

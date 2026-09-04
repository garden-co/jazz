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
use futures::SinkExt as _;
use jazz::db::{CommitUnitTrust, ConnectionSessionContext};
use jazz::groove::records::Value as CoreValue;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol_limits::MAX_WIRE_FRAME_BYTES;
use jazz::serving::ServerLinkAdmission;
use jazz::tools::Session;
use jazz::wire::{
    FEATURE_SYNC_MESSAGE_PAYLOAD, WireAuthorityEndpoint, WireError, WireErrorCode, WireFrame,
    WireHello, WirePeerRole, WireRetry, current_wire_features, encode_frame, negotiate_wire,
};
use tokio::sync::mpsc;

use crate::server::ServerState;

const WS_REQUIRED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD;
const WS_HANDSHAKE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const WS_PER_IDENTITY_CONNECTION_CAP: usize = crate::server::PER_CLIENT_CONNECTION_CAP;
const WS_MAX_FRAME_BYTES: usize = MAX_WIRE_FRAME_BYTES;
const WS_MAX_MESSAGE_BYTES: usize = WS_MAX_FRAME_BYTES;

static WS_NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
static WS_NEXT_CONNECTION_EPOCH: AtomicU64 = AtomicU64::new(1);
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
            axum::Json(jazz::tools::transport_error::ErrorResponse::internal(
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
    identity: AuthorSubject,
    claims: BTreeMap<String, CoreValue>,
    trust: CommitUnitTrust,
    credential: WebSocketCredential,
    requested_link: RequestedWebSocketLink,
}

/// Authentication class selected by the prelude.  `TrustedBackend` is still
/// the normal commit-ingest trust level for both machine credentials, but the
/// privileged catalogue bootstrap has a narrower authority boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WebSocketCredential {
    Admin,
    Backend,
    Session,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct WebSocketAdmissionKey {
    app_id: jazz::tools::AppId,
    identity: AuthorSubject,
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
    /// Present only for a public session. Trusted backend links are not part of
    /// the per-session connection cap: one edge legitimately owns multiple
    /// short-lived bootstrap and long-lived replication sockets under SYSTEM.
    key: Option<WebSocketAdmissionKey>,
    id: u64,
    evict_rx: mpsc::UnboundedReceiver<WebSocketEviction>,
    /// Keeps an unbounded registration's receiver pending without retaining a
    /// global admission-registry entry.
    _unbounded_keepalive: Option<mpsc::UnboundedSender<WebSocketEviction>>,
}

impl Drop for WebSocketAdmissionRegistration {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            ws_unregister_admission(key, self.id);
        }
    }
}

fn ws_admission_registry() -> &'static std::sync::Mutex<WebSocketAdmissionRegistry> {
    WS_ADMISSIONS.get_or_init(Default::default)
}

fn ws_register_admission(
    key: WebSocketAdmissionKey,
    enforce_session_cap: bool,
) -> WebSocketAdmissionRegistration {
    if !enforce_session_cap {
        let (keepalive, evict_rx) = mpsc::unbounded_channel();
        return WebSocketAdmissionRegistration {
            key: None,
            id: 0,
            evict_rx,
            _unbounded_keepalive: Some(keepalive),
        };
    }
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

    WebSocketAdmissionRegistration {
        key: Some(key),
        id,
        evict_rx,
        _unbounded_keepalive: None,
    }
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
    auth: jazz::tools::websocket_prelude_auth::AuthConfig,
    /// A one-shot authenticated authority-catalogue transfer. This is not a
    /// subscriber session and never admits application frames.
    #[serde(default)]
    bootstrap_catalogue: bool,
    /// Requested before any wire frame.  It is only a request: after JWT/cookie
    /// authentication and feature negotiation the server either creates its
    /// own immutable admitted relay capability or rejects the connection.
    #[serde(default)]
    requested_link: RequestedWebSocketLink,
}

/// The only client-selectable *request* at the WebSocket boundary.  This is
/// intentionally separate from `WirePeerRole`: a wire hello says what a peer
/// implements, not what authority it receives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum RequestedWebSocketLink {
    #[default]
    OrdinarySession,
    ScopeIsolatedClientRelay,
}

async fn ws_admission(
    prelude: WebSocketPrelude,
    request_headers: &HeaderMap,
    state: &Arc<ServerState>,
) -> Result<WebSocketAdmission, String> {
    let peer_identity = ws_peer_identity(&prelude.peer_identity)?;
    let requested_link = prelude.requested_link;
    let auth = prelude.auth;

    if let Some(admin_secret) = auth.admin_secret.as_deref() {
        crate::middleware::auth::validate_admin_secret(Some(admin_secret), &state.auth_config)
            .map_err(|(_, message)| message.to_owned())?;
        // An admin credential authenticates Edge's control plane, but ordinary
        // relay commits must retain their transaction permission subject for
        // application-policy evaluation. Complete authority publications have
        // their own prior-edge-admission capability, never inferred from SYSTEM
        // or from an ordinary backend credential.
        let trust = if prelude.bootstrap_catalogue
            && peer_identity == AuthorSubject::SYSTEM
            && state.topology == crate::server::ServerTopology::Core
        {
            CommitUnitTrust::TrustedAdmin
        } else {
            CommitUnitTrust::TrustedAuthority
        };
        return Ok(WebSocketAdmission {
            identity: peer_identity,
            claims: BTreeMap::new(),
            trust,
            credential: WebSocketCredential::Admin,
            requested_link: RequestedWebSocketLink::OrdinarySession,
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
    let has_authenticated_backend_session = has_session_header
        && matches!(
            (
                state.auth_config.backend_secret.as_deref(),
                backend_secret,
            ),
            (Some(expected), Some(provided)) if expected == provided
        );
    if backend_secret.is_some() && !has_jwt && !has_session_header {
        crate::middleware::auth::validate_backend_secret(backend_secret, &state.auth_config)
            .map_err(|(_, message)| message.to_owned())?;
        return Ok(WebSocketAdmission {
            identity: peer_identity,
            claims: BTreeMap::new(),
            trust: CommitUnitTrust::TrustedBackend,
            credential: WebSocketCredential::Backend,
            requested_link: RequestedWebSocketLink::OrdinarySession,
        });
    }

    if !has_jwt
        && !has_authenticated_backend_session
        && ws_has_auth_cookie(&headers, state.auth_config.auth_cookie_name.as_deref())
    {
        validate_ws_cookie_origin(&headers, state.auth_config.trust_forwarded_host)?;
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

    ws_validate_session_identity(&session, peer_identity)?;
    Ok(WebSocketAdmission {
        identity: peer_identity,
        claims: session_claims(session)?,
        trust: CommitUnitTrust::Session,
        credential: WebSocketCredential::Session,
        requested_link,
    })
}

fn session_claims(
    session: jazz::tools::public_schema::Session,
) -> Result<BTreeMap<String, CoreValue>, String> {
    let author = session
        .author_subject()
        .map_err(|error| error.to_string())?;
    let provider_claims = match session.claims {
        serde_json::Value::Object(map) => {
            // Middleware exposes these convenient aliases to server handlers,
            // but they duplicate the verified transport identity. A relay
            // capability must use the one canonical binding vocabulary: the
            // shared admission constructor derives `claims.iss`/`claims.sub`,
            // `user`, and `authMode` from the verified AuthorSubject.
            map.into_iter().collect()
        }
        _ => BTreeMap::new(),
    };
    let provider_claims =
        jazz::serving::auth_admission::jwt_json_claims_to_policy_claims(provider_claims)
            .map_err(|error| error.to_string())?;
    Ok(jazz::serving::auth_admission::admitted_session_claims(
        &session.issuer,
        &session.user_id,
        author,
        provider_claims,
    ))
}

fn ws_peer_identity(identity: &str) -> Result<AuthorSubject, String> {
    AuthorSubject::from_canonical(identity).map_err(|error| error.to_string())
}

fn ws_validate_session_identity(
    session: &Session,
    peer_identity: AuthorSubject,
) -> Result<(), String> {
    let session_identity = session
        .author_subject()
        .map_err(|error| error.to_string())?;
    if session_identity != peer_identity {
        return Err("websocket peer_identity must match authenticated session author".to_owned());
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

fn validate_ws_cookie_origin(
    headers: &HeaderMap,
    trust_forwarded_host: bool,
) -> Result<(), String> {
    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "cookie websocket auth requires Origin header".to_owned())?;
    let host = ws_cookie_origin_host(headers, trust_forwarded_host)
        .ok_or_else(|| "cookie websocket auth requires Host header".to_owned())?;

    if ws_origin_matches_host(origin, host) {
        return Ok(());
    }
    Err("cookie websocket auth Origin does not match Host".to_owned())
}

fn ws_cookie_origin_host(headers: &HeaderMap, trust_forwarded_host: bool) -> Option<&str> {
    let forwarded_host = trust_forwarded_host
        .then(|| {
            headers
                .get("X-Forwarded-Host")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(',').next())
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .flatten();
    forwarded_host.or_else(|| {
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
    let bootstrap_catalogue = prelude.bootstrap_catalogue;
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
    // This cap is deliberately scoped to externally authenticated sessions,
    // after credential verification.  It must not key off `SYSTEM` (or any
    // other claimed subject): trusted edge/bootstrap links share SYSTEM and a
    // single edge may transiently hold several such connections while
    // reconnecting.  Reserved subjects are rejected by `ws_admission` before
    // reaching this point.
    let mut admission_registration = ws_register_admission(
        WebSocketAdmissionKey {
            app_id: state.app_id,
            identity: admission.identity,
        },
        admission.credential == WebSocketCredential::Session,
    );

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

    let negotiated = match negotiate_wire(&remote_hello, current_wire_features()) {
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
    // A downstream browser may be authority-unbound while still accepting the
    // server's authenticated authority in its response Hello.  The server
    // never installs scoped authority semantics without an admitted remote
    // endpoint (below), so this directional capability advertisement does not
    // turn a client self-assertion into authority proof.

    if bootstrap_catalogue {
        if admission.credential != WebSocketCredential::Admin
            || admission.identity != AuthorSubject::SYSTEM
            || state.topology != crate::server::ServerTopology::Core
        {
            send_ws_error(
                &mut socket,
                WireError::new(
                    WireErrorCode::AuthFailed,
                    WireRetry::Never,
                    "catalogue bootstrap requires the authenticated core authority",
                ),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
        let Some(core_server_shell) = state.runtime() else {
            send_ws_error(
                &mut socket,
                WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Later,
                    "authority runtime is not ready to provide its catalogue",
                ),
            )
            .await;
            let _ = socket.close().await;
            return;
        };
        let server_endpoint = WireAuthorityEndpoint {
            node: NodeUuid::from_bytes([0x5e; 16]),
            epoch: WS_NEXT_CONNECTION_EPOCH.fetch_add(1, Ordering::Relaxed),
        };
        let hello = match encode_frame(&WireFrame::Hello(
            WireHello::current(WirePeerRole::Core, negotiated.features)
                .with_authority(server_endpoint.node, server_endpoint.epoch),
        )) {
            Ok(frame) => frame,
            Err(error) => {
                send_ws_error(
                    &mut socket,
                    WireError::new(
                        WireErrorCode::Internal,
                        WireRetry::Never,
                        format!("failed to encode bootstrap hello: {error}"),
                    ),
                )
                .await;
                let _ = socket.close().await;
                return;
            }
        };
        if send_ws_encoded_frames(&mut socket, &[hello]).await.is_err() {
            return;
        }
        let frames = match core_server_shell
            .encoded_trusted_catalogue_snapshot(negotiated.protocol_version, negotiated.features)
            .await
        {
            Ok(frames) => frames,
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
        let _ = send_ws_encoded_frames(&mut socket, &frames).await;
        let _ = socket.close().await;
        return;
    }

    let Some(core_server_shell) = state.runtime_for_client() else {
        send_ws_error(
            &mut socket,
            WireError::new(
                WireErrorCode::NotReady,
                WireRetry::Later,
                "runtime is bootstrapping its authoritative catalogue; retry shortly",
            ),
        )
        .await;
        let _ = socket.close().await;
        return;
    };
    // Every admitted server link receives a fresh server endpoint. A browser
    // client need not (and must not) self-assert one merely to learn which
    // authority issued its downstream fates.
    let server_endpoint = WireAuthorityEndpoint {
        node: NodeUuid::from_bytes([0x5e; 16]),
        epoch: WS_NEXT_CONNECTION_EPOCH.fetch_add(1, Ordering::Relaxed),
    };
    let session_context = if negotiated.features
        & (jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
            | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS)
        != 0
    {
        remote_hello
            .authority
            .map(|remote| ConnectionSessionContext {
                local: server_endpoint,
                remote,
                link_identity: admission.identity,
                negotiated_features: negotiated.features,
            })
    } else {
        None
    };
    let link_admission = match admission.requested_link {
        RequestedWebSocketLink::OrdinarySession => ServerLinkAdmission::OrdinarySession,
        RequestedWebSocketLink::ScopeIsolatedClientRelay
            if admission.credential == WebSocketCredential::Session
                && negotiated.features & jazz::wire::FEATURE_SCOPE_ISOLATED_CLIENT_RELAY != 0 =>
        {
            // This epoch was minted by the server for this accepted socket.
            // Reconnects necessarily get a fresh capability.
            ServerLinkAdmission::ScopeIsolatedClientRelay {
                admission_epoch: server_endpoint.epoch,
            }
        }
        RequestedWebSocketLink::ScopeIsolatedClientRelay => {
            send_ws_error(
                &mut socket,
                WireError::new(
                    WireErrorCode::UnsupportedFeature,
                    WireRetry::Never,
                    "scope-isolated client relay requires an authenticated session and negotiated relay feature",
                ),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    let session = match core_server_shell
        .open_with_session_context(
            admission.identity,
            admission.claims,
            admission.trust,
            negotiated.features,
            session_context,
            link_admission,
        )
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
    let server_hello = WireFrame::Hello(
        WireHello::current(WirePeerRole::Core, negotiated.features)
            .with_authority(server_endpoint.node, server_endpoint.epoch),
    );
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

    'connection: loop {
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
                    // The shell owns a synchronous database, so one tick is the
                    // smallest ordering-preserving unit at which its newly
                    // durable responses can be observed. Do not hold those
                    // responses behind the rest of this WebSocket message: a
                    // large import can otherwise delay an already-global
                    // FateUpdate until every later commit has been ingested.
                    let mut outbound = match core_server_shell.receive_tick_stream(session, frames) {
                        Ok(outbound) => outbound,
                        Err(error) => {
                            send_ws_error(
                                &mut socket,
                                WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
                            )
                            .await;
                            break 'connection;
                        }
                    };
                    while let Some(outbound) = outbound.recv().await {
                        let outbound = match outbound {
                            Ok(frames) => frames,
                            Err(error) => {
                                send_ws_error(
                                    &mut socket,
                                    WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
                                )
                                .await;
                                break 'connection;
                            }
                        };
                        if !outbound.is_empty()
                            && let Err(error) = send_ws_encoded_frames(&mut socket, &outbound).await
                        {
                            send_ws_error(
                                &mut socket,
                                WireError::new(
                                    WireErrorCode::Internal,
                                    WireRetry::Later,
                                    error.to_string(),
                                ),
                            )
                            .await;
                            break 'connection;
                        }
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
    core_server_shell: &crate::server::ServerRuntimeHandle,
    session: jazz::serving::ServerSession,
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
    match decode_ws_frame_batch(bytes) {
        Ok(mut frames) if frames.len() == 1 => Ok(frames.remove(0)),
        Ok(_) => Err(postcard::Error::DeserializeBadEncoding),
        Err(_) => jazz::wire::decode_frame(bytes),
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
    jazz::wire::decode_websocket_frame_batch(bytes)
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
        socket.send(Message::Binary(batch.into())).await?;
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
        let mut candidate = current.clone();
        candidate.push(frame.clone());
        let candidate_fits = jazz::wire::encode_websocket_frame_batch(&candidate).is_ok();
        if !candidate_fits && !current.is_empty() {
            batches.push(jazz::wire::encode_websocket_frame_batch(&current)?);
            current.clear();
            // A singleton has its own count and length prefixes. Validate the
            // actual carrier after the flush instead of assuming a raw frame
            // at the frame limit can fit by itself.
            jazz::wire::encode_websocket_frame_batch(std::slice::from_ref(frame))?;
        } else if !candidate_fits {
            return Err(postcard::Error::SerializeBufferFull);
        }
        current.push(frame.clone());
    }
    if !current.is_empty() {
        batches.push(jazz::wire::encode_websocket_frame_batch(&current)?);
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
    use std::sync::Arc;
    use std::time::Duration;

    use futures::StreamExt as _;
    use futures::stream::FuturesUnordered;
    use jazz::db::{
        Db, DbConfig, DbIdentity, PreparedQuery, QueryAttachment, ReadOpts, RowCells,
        SeededRowIdSource, WireTransportAdapter, WriteHandle, WriteState,
    };
    use jazz::groove::storage::MemoryStorage as CoreMemoryStorage;
    use jazz::ids::NodeUuid;
    use jazz::protocol::SyncMessage;
    use jazz::protocol_limits::MAX_WIRE_BATCH_FRAMES;
    use jazz::schema::{JazzSchema, TableSchema};
    use jazz::tx::{DurabilityTier, Fate, RejectionReason, TxId};
    use jazz::wire::{
        FEATURE_MESSAGE_FRAGMENTATION, FEATURE_STRUCTURED_ERRORS, TransportError,
        WIRE_PROTOCOL_VERSION, WireMessageFragment, WireTransport,
    };
    use jazz::wire::{WireStreamDecoder, decode_frame, decode_sync_message};
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

    use crate::middleware::AuthConfig;
    use crate::server::{ServerBuilder, StorageBackend};
    use jazz::tools::public_schema::{
        ColumnType, PolicyExpr, Schema, SchemaBuilder, TablePolicies,
        TableSchema as PublicTableSchema,
    };
    use jazz::tools::{AppId, AuthMode};

    const WS_STORM_SIZE: usize = 24;

    fn session_for(identity: AuthorSubject) -> Session {
        let (issuer, subject): (String, String) =
            serde_json::from_str(identity.canonical()).expect("authenticated test subject");
        Session::new(issuer, subject)
    }

    fn issuer_and_subject(identity: AuthorSubject) -> (String, String) {
        serde_json::from_str(identity.canonical()).expect("authenticated test subject")
    }
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
    fn raw_handshake_frame_consumes_the_complete_carrier() {
        let frame = WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            current_wire_features(),
        ));
        let raw = encode_frame(&frame).expect("encode raw handshake frame");
        assert_eq!(decode_single_ws_frame(&raw).unwrap(), frame);

        let mut suffixed = raw;
        suffixed.push(0);
        assert!(
            decode_single_ws_frame(&suffixed).is_err(),
            "a raw frame plus a suffix must not acquire a second handshake interpretation"
        );
    }

    #[test]
    fn ws_peer_identity_requires_canonical_author_subject() {
        let identity =
            AuthorSubject::for_test_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(ws_peer_identity(identity.canonical()).unwrap(), identity);

        assert!(ws_peer_identity("not-hex").is_err());
    }

    #[test]
    fn ws_session_identity_must_match_peer_identity() {
        let peer = AuthorSubject::for_test_bytes([1; 16]);
        let matching = session_for(peer);
        let mismatching = session_for(AuthorSubject::for_test_bytes([2; 16]));
        let external_subject = "better-auth-user";
        let external_peer =
            AuthorSubject::authenticated("https://auth.example", external_subject).unwrap();
        let external_session = Session::new("https://auth.example", external_subject);

        assert!(ws_validate_session_identity(&matching, peer).is_ok());
        assert!(ws_validate_session_identity(&mismatching, peer).is_err());
        assert!(ws_validate_session_identity(&external_session, external_peer).is_ok());
        assert!(ws_validate_session_identity(&external_session, peer).is_err());
        assert!(ws_validate_session_identity(&external_session, AuthorSubject::SYSTEM).is_err());
        let same_subject_other_issuer =
            AuthorSubject::authenticated("https://other-auth.example", external_subject).unwrap();
        assert!(
            ws_validate_session_identity(&external_session, same_subject_other_issuer).is_err(),
            "a shared provider subject must not bridge issuer domains"
        );
        let local_first = Session::new(AuthorSubject::LOCAL_FIRST_ISSUER, "local-user")
            .with_auth_mode(AuthMode::LocalFirst);
        let local_first_peer =
            AuthorSubject::from_canonical(r#"["urn:jazz:local-first","local-user"]"#).unwrap();
        assert!(ws_validate_session_identity(&local_first, local_first_peer).is_ok());
        let anonymous = Session::new(AuthorSubject::ANONYMOUS_ISSUER, "anonymous-user")
            .with_auth_mode(AuthMode::Anonymous);
        let anonymous_peer =
            AuthorSubject::from_canonical(r#"["urn:jazz:anonymous","anonymous-user"]"#).unwrap();
        assert!(ws_validate_session_identity(&anonymous, anonymous_peer).is_ok());
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
    fn websocket_session_claims_use_canonical_and_collision_proof_namespaces() {
        let session = Session::new("https://issuer.example", "verified-subject").with_claims(
            serde_json::json!({
                "user": "provider-spoof",
                "role": "writer",
                "iss": "spoofed-issuer",
                "sub": "spoofed-subject",
                "issuer": "application-issuer",
                "subject": "application-subject",
                "authMode": "spoofed-mode",
                "score": 7
            }),
        );
        let claims = session_claims(session).expect("admit websocket claims");

        assert_eq!(
            claims.get("user"),
            Some(&CoreValue::String(
                r#"["https://issuer.example","verified-subject"]"#.to_owned()
            ))
        );
        assert_eq!(
            claims.get("\0claims:user"),
            Some(&CoreValue::String("provider-spoof".to_owned()))
        );
        assert_eq!(
            claims.get("\0claims:role"),
            Some(&CoreValue::String("writer".to_owned()))
        );
        assert_eq!(claims.get("\0claims:score"), Some(&CoreValue::U64(7)));
        assert_eq!(
            claims.get("\0claims:authMode"),
            Some(&CoreValue::String("spoofed-mode".to_owned()))
        );
        assert_eq!(
            claims.get("\0claims:issuer"),
            Some(&CoreValue::String("application-issuer".to_owned()))
        );
        assert_eq!(
            claims.get("\0claims:subject"),
            Some(&CoreValue::String("application-subject".to_owned()))
        );
        assert_eq!(
            claims.get("authMode"),
            Some(&CoreValue::String("external".to_owned()))
        );
        assert_eq!(
            claims.get("\0claims:iss"),
            Some(&CoreValue::String("https://issuer.example".to_owned()))
        );
        assert_eq!(
            claims.get("\0claims:sub"),
            Some(&CoreValue::String("verified-subject".to_owned()))
        );
        for forbidden in [
            "role", "iss", "sub", "issuer", "subject", "user_id", "author",
        ] {
            assert!(
                !claims.contains_key(forbidden),
                "raw alias leaked: {forbidden}"
            );
        }
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
    fn bug_302_cookie_origin_ignores_forwarded_host_without_trusted_proxy() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::ORIGIN,
            "https://evil.example".parse().unwrap(),
        );
        headers.insert(axum::http::header::HOST, "app.example".parse().unwrap());
        headers.insert("X-Forwarded-Host", "evil.example".parse().unwrap());

        assert!(
            validate_ws_cookie_origin(&headers, false).is_err(),
            "an untrusted forwarded host must not bypass the cookie origin guard"
        );
    }

    #[test]
    fn ws_cookie_origin_uses_first_forwarded_host_when_trusted_proxy_enabled() {
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

        assert!(validate_ws_cookie_origin(&headers, true).is_ok());
    }

    #[test]
    fn ws_cookie_origin_rejects_missing_or_cross_origin() {
        assert!(!ws_origin_matches_host(
            "https://evil.example",
            "app.example"
        ));

        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::HOST, "app.example".parse().unwrap());
        assert!(validate_ws_cookie_origin(&headers, false).is_err());

        headers.insert(
            axum::http::header::ORIGIN,
            "https://evil.example".parse().unwrap(),
        );
        assert!(validate_ws_cookie_origin(&headers, false).is_err());
    }

    #[tokio::test]
    async fn bug_302_orphan_backend_session_cannot_bypass_cookie_origin_check() {
        let app_id = AppId::random();
        let state = ServerBuilder::new(app_id)
            .with_auth_config(AuthConfig {
                auth_cookie_name: Some("jazz-auth".to_owned()),
                allow_local_first_auth: true,
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .with_schema(Schema::new())
            .build()
            .await
            .expect("build cookie auth websocket state")
            .state;
        let seed = [0x42; 32];
        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &seed,
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            &app_id.to_string(),
            3_600,
        )
        .expect("mint local-first cookie token");
        let user_id = jazz::tools::identity::derive_user_id(&seed).to_string();
        let peer_identity = AuthorSubject::from_canonical(
            &serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, user_id))
                .expect("encode local-first author"),
        )
        .expect("local-first peer identity");
        let prelude = WebSocketPrelude {
            peer_identity: peer_identity.canonical().to_owned(),
            bootstrap_catalogue: false,
            requested_link: RequestedWebSocketLink::OrdinarySession,
            auth: jazz::tools::websocket_prelude_auth::AuthConfig {
                backend_session: Some(serde_json::json!({ "attacker": true })),
                ..Default::default()
            },
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            format!("jazz-auth={token}").parse().unwrap(),
        );
        headers.insert(
            axum::http::header::ORIGIN,
            "https://evil.example".parse().unwrap(),
        );
        headers.insert(axum::http::header::HOST, "app.example".parse().unwrap());

        let error = ws_admission(prelude, &headers, &state)
            .await
            .expect_err("orphan backend session must not suppress cookie origin enforcement");
        assert!(error.contains("Origin does not match Host"), "{error}");
    }

    #[test]
    fn websocket_limits_match_the_wire_protocol_limit() {
        assert_eq!(WS_MAX_FRAME_BYTES, MAX_WIRE_FRAME_BYTES);
        assert_eq!(WS_MAX_MESSAGE_BYTES, MAX_WIRE_FRAME_BYTES);
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

    fn public_table_policies() -> TablePolicies {
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True)
    }

    fn ws_public_schema_convert() -> JazzSchema {
        let source = SchemaBuilder::new()
            .table(
                PublicTableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .policies(public_table_policies()),
            )
            .build();
        jazz::schema::JazzSchema::new(&source).expect("websocket public schema compiles")
    }

    fn compiled_table(schema: &JazzSchema, table: &str) -> TableSchema {
        schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .unwrap_or_else(|| panic!("compiled websocket schema contains {table}"))
            .clone()
    }

    fn ws_todos_table_schema() -> TableSchema {
        compiled_table(&ws_public_schema_convert(), "todos")
    }

    fn ws_private_docs_schema_convert() -> JazzSchema {
        let source = SchemaBuilder::new()
            .table(
                PublicTableSchema::builder("docs")
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(
                        public_table_policies()
                            .with_select(PolicyExpr::eq_session("owner", vec!["user".to_owned()])),
                    ),
            )
            .build();
        jazz::schema::JazzSchema::new(&source)
            .expect("websocket private docs public schema compiles")
    }

    fn ws_private_docs_table_schema() -> TableSchema {
        compiled_table(&ws_private_docs_schema_convert(), "docs")
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
    async fn ws_admin_authority_capability_is_distinct_from_backend_attribution() {
        let state = make_ws_test_state().await;
        let relay = ws_admission(
            WebSocketPrelude {
                peer_identity: AuthorSubject::SYSTEM.canonical().to_owned(),
                bootstrap_catalogue: false,
                requested_link: RequestedWebSocketLink::OrdinarySession,
                auth: jazz::tools::websocket_prelude_auth::AuthConfig {
                    admin_secret: Some("admin-secret".to_owned()),
                    ..Default::default()
                },
            },
            &HeaderMap::new(),
            &state,
        )
        .await
        .expect("admit authenticated edge relay");
        assert_eq!(relay.credential, WebSocketCredential::Admin);
        assert_eq!(relay.trust, CommitUnitTrust::TrustedAuthority);

        let bootstrap = ws_admission(
            WebSocketPrelude {
                peer_identity: AuthorSubject::SYSTEM.canonical().to_owned(),
                bootstrap_catalogue: true,
                requested_link: RequestedWebSocketLink::OrdinarySession,
                auth: jazz::tools::websocket_prelude_auth::AuthConfig {
                    admin_secret: Some("admin-secret".to_owned()),
                    ..Default::default()
                },
            },
            &HeaderMap::new(),
            &state,
        )
        .await
        .expect("admit authenticated catalogue bootstrap");
        assert_eq!(bootstrap.trust, CommitUnitTrust::TrustedAdmin);

        let non_system = ws_admission(
            WebSocketPrelude {
                peer_identity: AuthorSubject::for_test_bytes([0x77; 16])
                    .canonical()
                    .to_owned(),
                bootstrap_catalogue: true,
                requested_link: RequestedWebSocketLink::OrdinarySession,
                auth: jazz::tools::websocket_prelude_auth::AuthConfig {
                    admin_secret: Some("admin-secret".to_owned()),
                    ..Default::default()
                },
            },
            &HeaderMap::new(),
            &state,
        )
        .await
        .expect("admit authentication before protocol bootstrap rejection");
        assert_eq!(non_system.trust, CommitUnitTrust::TrustedAuthority);

        let backend = ws_admission(
            WebSocketPrelude {
                peer_identity: AuthorSubject::SYSTEM.canonical().to_owned(),
                bootstrap_catalogue: false,
                requested_link: RequestedWebSocketLink::OrdinarySession,
                auth: jazz::tools::websocket_prelude_auth::AuthConfig {
                    backend_secret: Some("backend-secret".to_owned()),
                    ..Default::default()
                },
            },
            &HeaderMap::new(),
            &state,
        )
        .await
        .expect("ordinary backend is authenticated, but has no prior-edge-admission proof");
        assert_eq!(backend.trust, CommitUnitTrust::TrustedBackend);
    }

    #[tokio::test]
    async fn ws_backend_session_must_match_peer_identity() {
        let state = make_ws_test_state().await;
        let authenticated = AuthorSubject::for_test_bytes([0x51; 16]);
        let forged_peer = AuthorSubject::for_test_bytes([0x52; 16]);
        let (issuer, user_id) = issuer_and_subject(authenticated);
        let prelude = WebSocketPrelude {
            peer_identity: forged_peer.canonical().to_owned(),
            bootstrap_catalogue: false,
            requested_link: RequestedWebSocketLink::OrdinarySession,
            auth: jazz::tools::websocket_prelude_auth::AuthConfig {
                backend_secret: Some("backend-secret".to_owned()),
                backend_session: Some(serde_json::json!({
                    "issuer": issuer,
                    "user_id": user_id,
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
            error.contains("peer_identity must match authenticated session author"),
            "unexpected websocket admission error: {error}"
        );
    }

    // Internal admission-boundary test: server-shell policy reads are not yet
    // observable through a public websocket client helper, so this pins
    // the security invariant at the route admission point that feeds
    // ServerRuntimeHandle::open(identity, claims, trust).
    #[tokio::test]
    async fn ws_backend_session_admits_session_claims_for_policy_reads() {
        let state = make_ws_test_state().await;
        let identity = AuthorSubject::for_test_bytes([0x61; 16]);
        let (issuer, user_id) = issuer_and_subject(identity);
        let prelude = WebSocketPrelude {
            peer_identity: identity.canonical().to_owned(),
            bootstrap_catalogue: false,
            requested_link: RequestedWebSocketLink::OrdinarySession,
            auth: jazz::tools::websocket_prelude_auth::AuthConfig {
                backend_secret: Some("backend-secret".to_owned()),
                backend_session: Some(serde_json::json!({
                    "issuer": issuer,
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
            admission.claims.get("\0claims:role"),
            Some(&CoreValue::String("reader".to_owned()))
        );
        assert_eq!(
            admission.claims.get("\0claims:teams"),
            Some(&CoreValue::Array(vec![
                CoreValue::String("eng".to_owned()),
                CoreValue::String("ops".to_owned()),
            ]))
        );
        assert_eq!(
            admission.claims.get("\0claims:beta"),
            Some(&CoreValue::Bool(true))
        );
        assert_eq!(
            admission.claims.get("\0claims:login_count"),
            Some(&CoreValue::U64(7))
        );
        assert_eq!(
            admission.claims.get("\0claims:sub"),
            Some(&CoreValue::String(user_id.clone()))
        );
        assert!(!admission.claims.contains_key("subject"));
        assert!(!admission.claims.contains_key("user_id"));
        // Auth metadata is derived from the admitted identity, not flattened
        // from caller claims. Arbitrary claims remain in their own namespace.
        assert_eq!(
            admission.claims.get("authMode"),
            Some(&CoreValue::String("external".to_owned()))
        );
        assert_eq!(
            admission.claims.get("user"),
            Some(&CoreValue::String(identity.canonical().to_owned()))
        );
        assert!(!admission.claims.contains_key("\0claims:authMode"));
    }

    // Internal route-boundary test: this proves the reusable core
    // websocket client helper negotiates the real /apps/<APP_ID>/ws route
    // without reintroducing the legacy SyncPayload websocket handler.
    #[cfg(any())]
    #[tokio::test]
    async fn core_websocket_transport_helper_negotiates_route_hello() {
        let state = make_ws_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;

        let transport = WebSocketTransport::connect(
            format!("http://{addr}"),
            state.app_id,
            AuthorSubject::for_test_bytes([0x41; 16]),
            jazz::tools::websocket_prelude_auth::AuthConfig {
                admin_secret: Some("admin-secret".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("websocket helper should negotiate server hello");
        let (protocol_version, features, session_context) =
            transport.negotiated_transport_metadata();
        let context = session_context.expect("receipt-capable route admission context");
        assert_eq!(
            context.link_identity,
            AuthorSubject::for_test_bytes([0x41; 16])
        );
        assert_eq!(context.local.node, NodeUuid::from_bytes([0x41; 16]));
        assert_eq!(context.remote.node, NodeUuid::from_bytes([0x5e; 16]));
        assert_ne!(context.local.epoch, 0);
        assert_ne!(context.remote.epoch, 0);

        let schema = ws_public_schema_convert();
        let column_families = schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let db = Db::open(
            DbConfig::new(
                schema,
                CoreMemoryStorage::new(&refs).expect("valid memory storage families"),
                DbIdentity {
                    node: NodeUuid::from_bytes([0x41; 16]),
                    author: AuthorSubject::for_test_bytes([0x41; 16]),
                },
            )
            .with_id_source(SeededRowIdSource::new(0x4100)),
        )
        .await
        .expect("open client helper client db");
        db.connect_upstream(Box::new(WireTransportAdapter::new_with_session_context(
            transport,
            protocol_version,
            features,
            None,
            Some(context),
        )))
        .await;
        db.tick()
            .expect("client helper transport should accept db upstream frames");
    }

    // Route-level regression for the shell wake boundary.  A client DB tick
    // commonly queues outbound work; that is not new work for the shell that
    // is already executing the tick, so it must not schedule another one.
    // Conversely a real server reply must wake the owner so the queued frame
    // is consumed and its query coverage becomes observable.
    #[cfg(any())]
    #[tokio::test(flavor = "current_thread")]
    async fn websocket_transport_wakes_only_for_inbound_db_work() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let base_url = format!("http://{addr}");
        let auth = jazz::tools::websocket_prelude_auth::AuthConfig {
            admin_secret: Some("admin-secret".to_owned()),
            ..Default::default()
        };

        // Settle a real handshake before exercising the raw outbound hook.
        // The assertion immediately after `send_frame` is deliberate: the old
        // implementation invoked this callback synchronously from that method.
        let outbound_wakes = Arc::new(AtomicUsize::new(0));
        let outbound_wake = {
            let wakes = Arc::clone(&outbound_wakes);
            Arc::new(move || {
                wakes.fetch_add(1, AtomicOrdering::SeqCst);
            }) as Arc<dyn Fn() + Send + Sync>
        };
        let mut outbound_probe = WebSocketTransport::connect_with_wake(
            &base_url,
            state.app_id,
            AuthorSubject::for_test_bytes([0x71; 16]),
            auth.clone(),
            outbound_wake,
        )
        .await
        .expect("negotiate outbound wake probe");
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(outbound_wakes.load(AtomicOrdering::SeqCst), 0);
        outbound_probe
            .send_frame(Vec::new())
            .expect("queue outbound probe frame");
        assert_eq!(
            outbound_wakes.load(AtomicOrdering::SeqCst),
            0,
            "outbound send_frame must not wake the already-active shell"
        );
        drop(outbound_probe);

        let inbound_wakes = Arc::new(AtomicUsize::new(0));
        let inbound_wake = {
            let wakes = Arc::clone(&inbound_wakes);
            Arc::new(move || {
                wakes.fetch_add(1, AtomicOrdering::SeqCst);
            }) as Arc<dyn Fn() + Send + Sync>
        };
        let transport = WebSocketTransport::connect_with_wake(
            &base_url,
            state.app_id,
            AuthorSubject::for_test_bytes([0x72; 16]),
            auth,
            inbound_wake,
        )
        .await
        .expect("negotiate inbound DB-work client");
        let (protocol_version, features, session_context) =
            transport.negotiated_transport_metadata();
        let schema = ws_public_schema_convert();
        let column_families = schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let db = Db::open(
            DbConfig::new(
                schema,
                CoreMemoryStorage::new(&refs).expect("valid memory storage families"),
                DbIdentity {
                    node: NodeUuid::from_bytes([0x72; 16]),
                    author: AuthorSubject::for_test_bytes([0x72; 16]),
                },
            )
            .with_id_source(SeededRowIdSource::new(0x7200)),
        )
        .await
        .expect("open client DB");
        db.connect_upstream(Box::new(WireTransportAdapter::new_with_session_context(
            transport,
            protocol_version,
            features,
            None,
            session_context,
        )))
        .await;
        let query = db
            .prepare_query(&db.table("todos"))
            .expect("prepare client query");
        let attachment = db
            .attach_query_with_opts(
                &query,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    ..Default::default()
                },
            )
            .expect("attach client query");
        db.tick().expect("send valid client DB work");

        tokio::time::timeout(WS_SETTLE_DEADLINE, async {
            while inbound_wakes.load(AtomicOrdering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server reply enqueues and wakes client DB work");

        let deadline = tokio::time::Instant::now() + WS_SETTLE_DEADLINE;
        while !db.query_attachment_is_covered(&attachment) && tokio::time::Instant::now() < deadline
        {
            db.tick().expect("deliver queued inbound DB work");
            tokio::task::yield_now().await;
        }
        assert!(
            db.query_attachment_is_covered(&attachment),
            "the woken client DB must deliver the server's query coverage"
        );
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

    fn ws_prelude(identity: AuthorSubject) -> Vec<u8> {
        serde_json::json!({
            "peer_identity": identity.canonical(),
            "auth": {
                "admin_secret": "admin-secret",
            },
        })
        .to_string()
        .into_bytes()
    }

    fn ws_session_prelude(identity: AuthorSubject) -> Vec<u8> {
        let (issuer, user_id) = issuer_and_subject(identity);
        serde_json::json!({
            "peer_identity": identity.canonical().to_owned(),
            "auth": {
                "backend_secret": "backend-secret",
                "backend_session": {
                    "issuer": issuer,
                    "user_id": user_id,
                    "claims": {},
                    "authMode": "external",
                }
            }
        })
        .to_string()
        .into_bytes()
    }

    fn ws_anonymous_prelude(app_id: AppId, seed: [u8; 32]) -> (AuthorSubject, Vec<u8>) {
        let audience = app_id.to_string();
        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &seed,
            jazz::tools::identity::ANONYMOUS_ISSUER,
            &audience,
            3600,
        )
        .expect("mint anonymous test token");
        let verified = jazz::tools::identity::verify_jazz_self_signed_proof(&token, &audience)
            .expect("verify anonymous test token");
        let canonical = serde_json::to_string(&(verified.issuer, verified.user_id.as_str()))
            .expect("serialise anonymous author");
        let identity =
            AuthorSubject::from_canonical(&canonical).expect("parse anonymous author subject");
        let prelude = serde_json::json!({
            "peer_identity": identity.canonical(),
            "auth": {
                "jwt_token": token,
            }
        })
        .to_string()
        .into_bytes();
        (identity, prelude)
    }

    fn ws_client_hello_batch_with_features(features: u64) -> Vec<u8> {
        let hello = WireFrame::Hello(WireHello::current(WirePeerRole::Client, features));
        let encoded = vec![encode_frame(&hello).expect("encode client hello")];
        postcard::to_allocvec(&encoded).expect("encode websocket hello batch")
    }

    #[tokio::test]
    async fn websocket_accepts_batches_between_legacy_and_wire_caps() {
        let state = make_ws_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let identity = AuthorSubject::for_test_bytes([0x7a; 16]);
        let features = FEATURE_SYNC_MESSAGE_PAYLOAD
            | FEATURE_STRUCTURED_ERRORS
            | FEATURE_MESSAGE_FRAGMENTATION;
        let mut ws = open_negotiated_ws_with_prelude_and_features(
            addr,
            &state,
            ws_prelude(identity),
            features,
        )
        .await;
        let fragment_payload_len = 512 * 1024;
        let logical_payload = vec![0x42; fragment_payload_len * 4];
        let message_digest = [0; 32];
        let encoded = (0..3)
            .map(|index| {
                let offset = index * fragment_payload_len;
                let fragment = WireMessageFragment {
                    protocol_version: WIRE_PROTOCOL_VERSION,
                    features,
                    session: None,
                    message_id: 1,
                    message_digest,
                    total_len: logical_payload.len() as u64,
                    offset: offset as u64,
                    payload: logical_payload[offset..offset + fragment_payload_len].to_vec(),
                };
                encode_frame(&WireFrame::MessageFragment(fragment))
                    .expect("encode large websocket fragment")
            })
            .collect::<Vec<_>>();
        let batch = postcard::to_allocvec(&encoded).expect("encode large websocket batch");
        assert!(batch.len() > 1 << 20);
        assert!(batch.len() <= MAX_WIRE_FRAME_BYTES);

        ws.send(WsMessage::Binary(batch.into()))
            .await
            .expect("send protocol-sized websocket batch");
        let ping = vec![0x51, 0x52, 0x53];
        ws.send(WsMessage::Ping(ping.clone().into()))
            .await
            .expect("ping after protocol-sized batch");
        let pong = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Pong(payload))) => break payload,
                    Some(Ok(WsMessage::Close(frame))) => {
                        panic!("websocket closed after protocol-sized batch: {frame:?}")
                    }
                    Some(Err(error)) => {
                        panic!("websocket failed after protocol-sized batch: {error}")
                    }
                    Some(Ok(_)) => {}
                    None => panic!("websocket ended after protocol-sized batch"),
                }
            }
        })
        .await
        .expect("wait for pong after protocol-sized batch");
        assert_eq!(pong.as_ref(), ping.as_slice());
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
    fn websocket_frame_batches_split_at_protocol_frame_count_cap() {
        let frames = vec![vec![0x42]; MAX_WIRE_BATCH_FRAMES + 1];
        let batches = encode_ws_frame_batches(&frames).expect("split bounded frame count");

        assert_eq!(batches.len(), 2);
        assert_eq!(
            decode_ws_encoded_frame_batch(&batches[0])
                .expect("decode first bounded batch")
                .len(),
            MAX_WIRE_BATCH_FRAMES
        );
        assert_eq!(
            decode_ws_encoded_frame_batch(&batches[1])
                .expect("decode remaining bounded batch")
                .len(),
            1
        );
    }

    #[test]
    fn websocket_frame_batches_never_emit_a_carrier_over_the_physical_cap() {
        let largest_singleton = vec![0x42; MAX_WIRE_FRAME_BYTES - 4];
        let batches = encode_ws_frame_batches(std::slice::from_ref(&largest_singleton))
            .expect("largest singleton carrier fits exactly");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), MAX_WIRE_FRAME_BYTES);
        assert_eq!(
            decode_ws_encoded_frame_batch(&batches[0]).expect("decode exact carrier"),
            vec![largest_singleton],
        );

        let raw_limit = vec![0x42; MAX_WIRE_FRAME_BYTES];
        assert!(
            encode_ws_frame_batches(std::slice::from_ref(&raw_limit)).is_err(),
            "raw limit frame alone cannot fit its carrier prefixes"
        );
        assert!(
            encode_ws_frame_batches(&[vec![0x11], raw_limit]).is_err(),
            "flushing an earlier frame must still validate the oversized singleton"
        );
    }

    #[test]
    fn websocket_frame_batch_decoder_rejects_empty_and_count_floods() {
        let empty = postcard::to_allocvec(&Vec::<Vec<u8>>::new()).expect("encode empty batch");
        assert!(decode_ws_encoded_frame_batch(&empty).is_err());

        let flood = postcard::to_allocvec(&vec![Vec::<u8>::new(); MAX_WIRE_BATCH_FRAMES + 1])
            .expect("encode count flood below physical byte cap");
        assert!(flood.len() <= MAX_WIRE_FRAME_BYTES);
        assert!(decode_ws_encoded_frame_batch(&flood).is_err());
    }

    #[test]
    fn websocket_frame_batch_decoder_consumes_the_complete_carrier() {
        let valid = [0x01, 0x01, 0x42];
        assert_eq!(
            postcard::to_allocvec(&vec![vec![0x42_u8]]).expect("encode valid batch"),
            valid,
            "frozen WebSocket batch corpus must remain canonical"
        );
        assert_eq!(
            decode_ws_encoded_frame_batch(&valid).expect("decode complete valid batch"),
            vec![vec![0x42]]
        );

        let mut suffixed = valid.to_vec();
        suffixed.push(0x00);
        assert!(
            decode_ws_encoded_frame_batch(&suffixed).is_err(),
            "a valid batch plus a suffix must not acquire a second interpretation"
        );
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
        identity: AuthorSubject,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        open_negotiated_ws_with_prelude(addr, state, ws_prelude(identity)).await
    }

    async fn open_negotiated_ws_session(
        addr: std::net::SocketAddr,
        state: &Arc<ServerState>,
        identity: AuthorSubject,
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
        open_negotiated_ws_with_prelude_and_features(
            addr,
            state,
            prelude,
            FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS,
        )
        .await
    }

    async fn open_negotiated_ws_with_prelude_and_features(
        addr: std::net::SocketAddr,
        state: &Arc<ServerState>,
        prelude: Vec<u8>,
        features: u64,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let (mut ws, _) = connect_async(ws_url(addr, state.app_id))
            .await
            .expect("connect websocket");
        ws.send(WsMessage::Binary(prelude.into()))
            .await
            .expect("send websocket prelude");
        ws.send(WsMessage::Binary(
            ws_client_hello_batch_with_features(features).into(),
        ))
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
        let frame = decode_frame(&frames[0]).expect("decode server hello");
        let WireFrame::Hello(server_hello) = frame else {
            panic!("expected server hello, got {frame:?}");
        };
        assert_eq!(server_hello.role, WirePeerRole::Core);
        assert!(
            server_hello.authority.is_some(),
            "an admitted server must bind its own downstream authority endpoint"
        );
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

    fn fate_tx_ids(decoder: &mut WireStreamDecoder, message: &WsMessage) -> Vec<TxId> {
        decode_ws_message(message)
            .into_iter()
            .filter_map(|frame| match frame {
                WireFrame::Message(envelope) => decoder
                    .decode_message(&envelope.payload, envelope.features)
                    .ok()
                    .and_then(|payload| decode_sync_message(&payload).ok())
                    .and_then(|message| match message {
                        SyncMessage::FateUpdate { tx_id, .. } => Some(tx_id),
                        _ => None,
                    }),
                WireFrame::Hello(_) | WireFrame::Error(_) | WireFrame::MessageFragment(_) => None,
            })
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
            Self::new_with_identity(
                schema,
                node_seed,
                row_seed,
                AuthorSubject::for_test_bytes([node_seed; 16]),
            )
            .await
        }

        async fn new_with_identity(
            schema: JazzSchema,
            node_seed: u8,
            row_seed: u64,
            author: AuthorSubject,
        ) -> Self {
            let column_families = schema.column_families();
            let refs = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let db = Db::open(
                DbConfig::new(
                    schema,
                    CoreMemoryStorage::new(&refs).expect("valid memory storage families"),
                    DbIdentity {
                        node: NodeUuid::from_bytes([node_seed; 16]),
                        author,
                    },
                )
                .with_id_source(SeededRowIdSource::new(row_seed)),
            )
            .await
            .expect("open client db");
            let transport = TestWireTransport::default();
            // Match the authority-unbound test hello's negotiated features.
            // Scoped semantics are not installed without an admitted remote
            // endpoint, even though a browser may accept the server endpoint
            // from its response Hello.
            db.connect_upstream(Box::new(WireTransportAdapter::new(
                transport.clone(),
                WIRE_PROTOCOL_VERSION,
                FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS,
                None,
            )))
            .await;
            Self {
                db,
                transport,
                todos_table: ws_todos_table_schema(),
            }
        }

        fn write_todo(&self, title: &str) -> WriteHandle<CoreMemoryStorage> {
            jazz::db::block_on(self.db.insert(
                "todos",
                RowCells::from([
                    ("title".to_owned(), CoreValue::String(title.to_owned())),
                    ("done".to_owned(), CoreValue::Bool(false)),
                ]),
                Default::default(),
            ))
            .expect("insert client row")
        }

        fn insert_todo(&self, title: &str) -> jazz::ids::RowUuid {
            self.write_todo(title).row_uuid()
        }

        fn write_todo_tx_id(&self, title: &str) -> TxId {
            self.write_todo(title).mergeable_tx_id()
        }

        fn update_todo(
            &self,
            row_uuid: jazz::ids::RowUuid,
            title: &str,
        ) -> WriteHandle<CoreMemoryStorage> {
            jazz::db::block_on(self.db.update(
                "todos",
                row_uuid,
                RowCells::from([
                    ("title".to_owned(), CoreValue::String(title.to_owned())),
                    ("done".to_owned(), CoreValue::Bool(false)),
                ]),
                Default::default(),
            ))
            .expect("update client row")
        }

        fn delete_todo(&self, row_uuid: jazz::ids::RowUuid) -> WriteHandle<CoreMemoryStorage> {
            jazz::db::block_on(self.db.delete("todos", row_uuid, Default::default()))
                .expect("delete client row")
        }

        fn insert_private_doc(&self, title: &str, owner: AuthorSubject) -> jazz::ids::RowUuid {
            let owner = owner.canonical().to_owned();
            jazz::db::block_on(self.db.insert(
                "docs",
                RowCells::from([
                    ("title".to_owned(), CoreValue::String(title.to_owned())),
                    ("owner".to_owned(), CoreValue::String(owner)),
                ]),
                Default::default(),
            ))
            .expect("insert client doc")
            .row_uuid()
        }

        fn tick_take(&self) -> Vec<Vec<u8>> {
            jazz::db::block_on(self.db.tick()).expect("tick client db");
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

    /// Unlike the transport pump's optional poll, setup has an explicit
    /// protocol obligation: a successfully admitted query session must first
    /// receive the authority catalogue. Wait for that required reply using the
    /// route test's normal bounded-settlement deadline.
    async fn receive_required_ws_encoded_frames(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Vec<Vec<u8>> {
        let message = tokio::time::timeout(WS_PUMP_DEADLINE, ws.next())
            .await
            .expect("server must answer initial websocket query setup before the deadline")
            .expect("server must keep the websocket open during initial query setup")
            .expect("server must send a valid initial websocket query setup response");
        let WsMessage::Binary(bytes) = message else {
            panic!("server must answer initial websocket query setup with binary wire frames");
        };
        postcard::from_bytes(&bytes).expect(
            "server must encode the initial websocket query setup response as a frame batch",
        )
    }

    async fn pump_core_websocket_transport_once(
        client: &TestClient,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> (usize, usize) {
        pump_core_websocket_transport_once_with_first_receive(client, ws, true).await
    }

    async fn pump_core_websocket_transport_once_with_first_receive(
        client: &TestClient,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        receive_after_client_send: bool,
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
            let inbound = if receive_after_client_send {
                try_receive_ws_encoded_frames(ws).await
            } else {
                Vec::new()
            };
            if inbound.is_empty() {
                outbound = client.tick_take();
            } else {
                received += inbound.len();
                outbound = client.receive_tick_take(inbound);
            }
        }
        // A server response may miss the short receive window immediately
        // following the client frame. Keep an idle pump bidirectional: without
        // this read, later pump calls with no client work would never observe
        // that already-queued response.
        if sent == 0 {
            received += receive_core_websocket_transport_push_once(client, ws).await;
        }
        (sent, received)
    }

    async fn settle_ws_write(
        client: &TestClient,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        write: &WriteHandle<CoreMemoryStorage>,
    ) -> WriteState {
        let start = tokio::time::Instant::now();
        loop {
            let _ = pump_core_websocket_transport_once(client, ws).await;
            let state = write.write_state().await.expect("websocket write state");
            if !matches!(state.fate, Fate::Pending) || start.elapsed() >= WS_SETTLE_DEADLINE {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

    /// Complete the protocol's catalogue/session setup before a route test
    /// attributes a later response to its own operation. A newly admitted
    /// session may first receive the authority catalogue, so treating that
    /// setup traffic as an operation response makes ordering tests test the
    /// handshake race rather than the stated route boundary.
    async fn settle_ws_todos_query(
        client: &TestClient,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> (PreparedQuery, QueryAttachment) {
        let (query, attachment) = client.attach_todos_query();
        let initial_outbound = client.tick_take();
        assert!(
            !initial_outbound.is_empty(),
            "query setup must send its initial wire frames"
        );
        ws.send(WsMessage::Binary(ws_frame_batch(&initial_outbound).into()))
            .await
            .expect("send initial websocket query setup");
        let initial_inbound = receive_required_ws_encoded_frames(ws).await;
        let mut decoder = WireStreamDecoder::new(current_wire_features())
            .expect("current wire compression must be available");
        let saw_catalogue = initial_inbound.iter().any(|frame| {
            let Ok(WireFrame::Message(envelope)) = decode_frame(frame) else {
                return false;
            };
            decoder
                .decode_message(&envelope.payload, envelope.features)
                .ok()
                .and_then(|payload| decode_sync_message(&payload).ok())
                .is_some_and(|message| matches!(message, SyncMessage::CatalogueSnapshot(_)))
        });
        assert!(
            saw_catalogue,
            "the first query setup response must carry the authority catalogue"
        );
        // Keep this exact observed setup response queued for the ordinary
        // client tick. The following pump therefore exercises the same
        // catalogue-then-query transition as a real connected client.
        client.transport.push_inbound(initial_inbound);
        let deadline = tokio::time::Instant::now() + WS_PUMP_DEADLINE;
        while !client.edge_attachment_is_covered(&attachment)
            && tokio::time::Instant::now() < deadline
        {
            let _ = pump_core_websocket_transport_once(client, ws).await;
            tokio::task::yield_now().await;
        }
        assert!(
            client.edge_attachment_is_covered(&attachment),
            "websocket setup must settle the initial query before testing a later operation"
        );
        (query, attachment)
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
        let mut ws_a =
            open_negotiated_ws(addr, &state, AuthorSubject::for_test_bytes([0xa1; 16])).await;
        let mut ws_b =
            open_negotiated_ws(addr, &state, AuthorSubject::for_test_bytes([0xb2; 16])).await;
        let (client_b_todos, client_b_todos_attachment) = client_b.attach_todos_query();
        let _inserted = client_a.insert_todo("route sync");

        let mut frames_sent_to_server = 0;
        let mut frames_received_from_server = 0;
        let expected_titles = vec!["route sync".to_owned()];
        let mut titles = Vec::new();
        let start = tokio::time::Instant::now();
        while start.elapsed() < WS_PUMP_DEADLINE {
            let (sent, received) = pump_core_websocket_transport_once(&client_a, &mut ws_a).await;
            frames_sent_to_server += sent;
            frames_received_from_server += received;
            let (sent, received) = pump_core_websocket_transport_once(&client_b, &mut ws_b).await;
            frames_sent_to_server += sent;
            frames_received_from_server += received;
            titles = client_b.edge_todo_titles(&client_b_todos).await;
            if client_b.edge_attachment_is_covered(&client_b_todos_attachment)
                && titles == expected_titles
            {
                break;
            }
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
        client_b.detach_query(client_b_todos_attachment);
        assert_eq!(
            titles, expected_titles,
            "the receiving client must materialize the row through the websocket route"
        );
    }
    #[tokio::test(flavor = "current_thread")]
    async fn anonymous_self_signed_session_is_read_only_over_raw_websocket_wire() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let schema = ws_public_schema_convert();

        let authenticated_identity = AuthorSubject::for_test_bytes([0xa1; 16]);
        let authenticated = TestClient::new(schema.clone(), 0xa1, 0xa100).await;
        let mut authenticated_ws =
            open_negotiated_ws_session(addr, &state, authenticated_identity).await;

        let (anonymous_identity, anonymous_prelude) =
            ws_anonymous_prelude(state.app_id, [0xb2; 32]);
        let anonymous =
            TestClient::new_with_identity(schema, 0xb2, 0xb200, anonymous_identity).await;
        let mut anonymous_ws =
            open_negotiated_ws_with_prelude(addr, &state, anonymous_prelude).await;
        let (anonymous_todos, anonymous_todos_attachment) =
            settle_ws_todos_query(&anonymous, &mut anonymous_ws).await;

        let permitted = authenticated.write_todo("permitted");
        let permitted_row = permitted.row_uuid();
        let permitted_state =
            settle_ws_write(&authenticated, &mut authenticated_ws, &permitted).await;
        assert_eq!(permitted_state.fate, Fate::Accepted);
        assert_eq!(permitted_state.durability, DurabilityTier::Global);

        let expected_titles = vec!["permitted".to_owned()];
        let start = tokio::time::Instant::now();
        let mut anonymous_titles = Vec::new();
        while start.elapsed() < WS_SETTLE_DEADLINE {
            let _ = pump_core_websocket_transport_once(&anonymous, &mut anonymous_ws).await;
            anonymous_titles = anonymous.edge_todo_titles(&anonymous_todos).await;
            if anonymous_titles == expected_titles {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            anonymous_titles, expected_titles,
            "anonymous sessions must retain public read access"
        );

        let denied_insert = anonymous.write_todo("must be denied");
        assert_eq!(
            settle_ws_write(&anonymous, &mut anonymous_ws, &denied_insert)
                .await
                .fate,
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            "the authority must reject anonymous inserts before permissive policy"
        );

        let denied_update = anonymous.update_todo(permitted_row, "must remain unchanged");
        assert_eq!(
            settle_ws_write(&anonymous, &mut anonymous_ws, &denied_update)
                .await
                .fate,
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            "the authority must reject anonymous updates before permissive policy"
        );

        let denied_delete = anonymous.delete_todo(permitted_row);
        assert_eq!(
            settle_ws_write(&anonymous, &mut anonymous_ws, &denied_delete)
                .await
                .fate,
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            "the authority must reject anonymous deletes before permissive policy"
        );

        assert_eq!(
            anonymous.edge_todo_titles(&anonymous_todos).await,
            expected_titles,
            "rejected anonymous writes must not alter the public settled view"
        );
        anonymous.detach_query(anonymous_todos_attachment);
    }

    // Internal route-boundary guard: WebSocket message boundaries are not
    // observable through the public JazzClient facade. Two real Db clients and
    // the public websocket route are therefore required to prove that a fate
    // already made Global is emitted before a later input frame is ingested.
    #[tokio::test(flavor = "current_thread")]
    async fn ws_flushes_early_global_fate_before_later_batch_frames() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let client = TestClient::new(ws_public_schema_convert(), 0xa1, 0xa100).await;
        let mut ws =
            open_negotiated_ws(addr, &state, AuthorSubject::for_test_bytes([0xa1; 16])).await;

        let (_, setup_attachment) = settle_ws_todos_query(&client, &mut ws).await;
        client.detach_query(setup_attachment);

        let early_tx = client.write_todo_tx_id("early fate");
        let mut final_tx = early_tx;
        for index in 0..32 {
            final_tx = client.write_todo_tx_id(&format!("later fate {index}"));
        }
        let outbound = client.tick_take();
        assert!(outbound.len() > 1, "import must contain later input frames");
        ws.send(WsMessage::Binary(ws_frame_batch(&outbound).into()))
            .await
            .expect("send one batched import message");

        let first_response = ws
            .next()
            .await
            .expect("server response while later frames remain")
            .expect("valid websocket response");
        let mut decoder = WireStreamDecoder::new(current_wire_features())
            .expect("current wire compression must be available");
        let first_fates = fate_tx_ids(&mut decoder, &first_response);
        assert!(
            first_fates.contains(&early_tx),
            "the first server response must include the already-global early transaction; frames={:?}",
            decode_ws_message(&first_response)
        );
        assert!(
            !first_fates.contains(&final_tx),
            "the final transaction must not be ingested before the early fate is flushed"
        );

        let mut observed_final = false;
        while !observed_final {
            let response = ws
                .next()
                .await
                .expect("server continues ingesting the batch")
                .expect("valid websocket response");
            observed_final = fate_tx_ids(&mut decoder, &response).contains(&final_tx);
        }
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
        let mut ws_b =
            open_negotiated_ws(addr, &state, AuthorSubject::for_test_bytes([0xb2; 16])).await;
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
        let client_a = TestClient::new(schema, 0xa1, 0xa100).await;
        let mut ws_a =
            open_negotiated_ws(addr, &state, AuthorSubject::for_test_bytes([0xa1; 16])).await;
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
        client_b.detach_query(client_b_todos_attachment);
    }

    // Internal route-boundary guard: public client APIs do not expose a way to
    // deliberately skip one websocket read. This forces that scheduling edge
    // and proves an idle transport pump still consumes the queued response.
    #[tokio::test(flavor = "current_thread")]
    async fn ws_idle_pump_drains_response_after_missed_first_receive() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let client = TestClient::new(ws_public_schema_convert(), 0xb2, 0xb200).await;
        let mut ws =
            open_negotiated_ws(addr, &state, AuthorSubject::for_test_bytes([0xb2; 16])).await;

        let (_, setup_attachment) = settle_ws_todos_query(&client, &mut ws).await;
        client.detach_query(setup_attachment);
        let (_todos, attachment) = client.attach_todos_query();

        let (sent, received) =
            pump_core_websocket_transport_once_with_first_receive(&client, &mut ws, false).await;
        assert!(
            sent > 0,
            "the query registration must reach the websocket route"
        );
        assert_eq!(
            received, 0,
            "the first pump deliberately skips its response"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !client.edge_attachment_is_covered(&attachment),
            "the queued response must not be applied before the idle pump reads it"
        );

        let (sent, received) = pump_core_websocket_transport_once(&client, &mut ws).await;
        assert_eq!(sent, 0, "the second pump must have no new client work");
        assert!(
            received > 0,
            "the idle pump must consume the queued response"
        );
        assert!(
            client.edge_attachment_is_covered(&attachment),
            "the drained server response must cover the registered query"
        );
        client.detach_query(attachment);
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
        let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
        let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
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
        let identity = AuthorSubject::for_test_bytes([0x42; 16]);
        let key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut sockets = Vec::new();
        for _ in 0..WS_PER_IDENTITY_CONNECTION_CAP {
            sockets.push(open_negotiated_ws_session(addr, &state, identity).await);
        }

        let mut oldest = sockets.remove(0);
        let _newest = open_negotiated_ws_session(addr, &state, identity).await;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn trusted_links_do_not_consume_the_public_session_connection_cap() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let identity = AuthorSubject::SYSTEM;
        let key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut links = Vec::new();
        for _ in 0..=WS_PER_IDENTITY_CONNECTION_CAP {
            links.push(open_negotiated_ws(addr, &state, identity).await);
        }

        assert_eq!(
            ws_live_admissions_for(key),
            0,
            "verified trusted links must not consume the untrusted per-session cap"
        );
        assert_eq!(links.len(), WS_PER_IDENTITY_CONNECTION_CAP + 1);
    }

    // Internal route-boundary test: websocket peer admission is not
    // observable through the public JazzClient API yet, so this tests the
    // protocol boundary and its admission registry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn peer_identity_storm_is_bounded_without_rejecting_newest_connections() {
        let state = make_ws_convergence_test_state().await;
        let addr = start_ws_test_server(state.clone()).await;
        let identity = AuthorSubject::for_test_bytes([0x24; 16]);
        let key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut pending = FuturesUnordered::new();
        for _ in 0..WS_STORM_SIZE {
            pending.push(open_negotiated_ws_session(addr, &state, identity));
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
        let noisy_identity = AuthorSubject::for_test_bytes([0x31; 16]);
        let quiet_identity = AuthorSubject::for_test_bytes([0x32; 16]);
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
            quiet_sockets.push(open_negotiated_ws_session(addr, &state, quiet_identity).await);
        }
        assert_eq!(
            ws_live_admissions_for(quiet_key),
            WS_PER_IDENTITY_CONNECTION_CAP
        );

        let mut pending = FuturesUnordered::new();
        for _ in 0..WS_STORM_SIZE {
            pending.push(open_negotiated_ws_session(addr, &state, noisy_identity));
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
        let identity = AuthorSubject::for_test_bytes([0x33; 16]);
        let key = WebSocketAdmissionKey {
            app_id: state.app_id,
            identity,
        };

        let mut sockets = Vec::new();
        for _ in 0..WS_PER_IDENTITY_CONNECTION_CAP {
            sockets.push(open_negotiated_ws_session(addr, &state, identity).await);
        }
        assert_eq!(
            wait_for_ws_live_admissions(key, |count| { count == WS_PER_IDENTITY_CONNECTION_CAP })
                .await,
            WS_PER_IDENTITY_CONNECTION_CAP
        );

        for cycle in 0..(WS_PER_IDENTITY_CONNECTION_CAP * 3) {
            sockets.push(open_negotiated_ws_session(addr, &state, identity).await);
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

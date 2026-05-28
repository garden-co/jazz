//! Behavioural tests for jaz0-a803 (reconnect-storm amplification).
//!
//! These tests drive the wire-level behaviour of a misconfigured or
//! malicious client and assert what the server must guarantee, without
//! peeking at the structure of the mitigation. They cover the two
//! mitigations selected for this branch:
//!
//! 1. **Per-`client_id` connection cap** with evict-oldest semantics:
//!    a single `client_id` cannot pin unbounded server state by opening
//!    many concurrent sockets, and a reconnecting client always wins
//!    against its own stale sockets.
//! 2. **Handshake-stage read timeout**: a WS upgrade that never sends
//!    an `AuthHandshake` frame cannot tie up server resources indefinitely.
//!
//! ## What these tests prove (and what they don't)
//!
//! The first and third tests are **exploit-primitive demonstrations at
//! sub-OOM scale**. They show that the server accepts the relevant input
//! pattern (many concurrent sockets under one `client_id`; a parked WS
//! upgrade that never handshakes) without applying any bound. They are
//! intentionally small enough to run quickly in CI — they prove the
//! primitive exists, not that it crashes a production host. Severity at
//! scale comes from the analysis on the ticket, not from these tests.
//!
//! The second test is a **fix-shape contract**, not an exploit demo. It
//! pins the eviction policy to "evict oldest, not reject newest" so a
//! reconnecting client cannot be locked out by its own zombies — that's
//! a UX correctness property of the chosen mitigation, not an attack.
//!
//! Each test observes the server only through the public WebSocket
//! protocol and the live `ServerState::connections` map — never through
//! internals of the cap itself — so it remains valid under any
//! implementation that satisfies the behavioural contract.

use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{SinkExt as _, StreamExt as _};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as WsMessage,
};

use crate::middleware::AuthConfig;
use crate::schema_manager::AppId;
use crate::server::{ServerBuilder, ServerState, StorageBackend};
use crate::sync_manager::ClientId;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Selected cap value for this branch (see ticket jaz0-a803).
const PER_CLIENT_CONNECTION_CAP: usize = 4;

/// Scale used by the exploit-primitive demonstrations. Big enough that the
/// failure message ("the server allowed N connections under one client_id"
/// / "the server kept N idle upgrades parked") reads as a real exploit
/// demonstration; small enough to run quickly inside CI's `ulimit -n`.
const STORM_SIZE: usize = 100;

const BACKEND_SECRET: &str = "test-backend-secret";

async fn make_test_state() -> Arc<ServerState> {
    let auth_config = AuthConfig {
        backend_secret: Some(BACKEND_SECRET.to_string()),
        ..Default::default()
    };
    ServerBuilder::new(AppId::from_name("test-app"))
        .with_auth_config(auth_config)
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .expect("build test server state")
        .state
}

async fn start_test_server(state: Arc<ServerState>) -> std::net::SocketAddr {
    let app = super::create_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("test listener addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });
    addr
}

/// Open a WS connection, send the handshake using `client_id_str`, and
/// wait for `ConnectedResponse`. The returned stream is fully registered
/// server-side by the time this resolves.
async fn open_authenticated_ws(
    addr: std::net::SocketAddr,
    state: &Arc<ServerState>,
    client_id_str: &str,
) -> Result<WsStream, String> {
    let url = format!("ws://{addr}/apps/{}/ws", state.app_id);
    let (mut ws, _) = connect_async(&url)
        .await
        .map_err(|e| format!("ws upgrade failed: {e}"))?;

    let handshake = crate::transport_manager::AuthHandshake {
        sync_protocol_version: crate::transport_manager::SYNC_PROTOCOL_VERSION,
        client_id: client_id_str.to_string(),
        auth: crate::transport_manager::AuthConfig {
            backend_secret: Some(BACKEND_SECRET.to_string()),
            ..Default::default()
        },
        catalogue_state_hash: None,
        declared_schema_hash: None,
    };
    let payload = serde_json::to_vec(&handshake).expect("serialize handshake");
    ws.send(WsMessage::Binary(
        crate::transport_manager::frame_encode(&payload).into(),
    ))
    .await
    .map_err(|e| format!("ws send handshake: {e}"))?;

    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timed out waiting for ConnectedResponse".to_string())?
        .ok_or_else(|| "ws stream ended before ConnectedResponse".to_string())?
        .map_err(|e| format!("ws recv: {e}"))?;
    let WsMessage::Binary(bytes) = msg else {
        return Err(format!("unexpected pre-Connected message: {msg:?}"));
    };
    let inner = crate::transport_manager::frame_decode(&bytes)
        .ok_or_else(|| "malformed pre-Connected frame".to_string())?;
    let _: crate::transport_manager::ConnectedResponse = serde_json::from_slice(&inner)
        .map_err(|e| format!("expected ConnectedResponse, got error: {e}"))?;
    Ok(ws)
}

async fn live_connections_for(state: &Arc<ServerState>, client_id: ClientId) -> usize {
    state
        .connections
        .read()
        .await
        .values()
        .filter(|c| c.client_id == client_id)
        .count()
}

/// EXPLOIT-PRIMITIVE: at sub-OOM scale, demonstrate that the server has no
/// bound on concurrent connections per `client_id`. A single client_id
/// can register `STORM_SIZE` fan-out targets in `ConnectionEventHub`,
/// each with its own unbounded outbound channel — payload memory grows
/// as `connections × per-channel backlog`, and the same primitive at
/// larger scale OOMs the server (see ticket analysis).
///
/// Today: all `STORM_SIZE` sockets register. The cap must reduce the
/// live count to at most `PER_CLIENT_CONNECTION_CAP`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_client_id_concurrent_connections_are_bounded() {
    let state = make_test_state().await;
    let addr = start_test_server(state.clone()).await;
    let client_id_str = ClientId::new().to_string();
    let client_id = ClientId::parse(&client_id_str).expect("parse client_id");

    // Open many concurrent sockets under the same client_id. We use
    // FuturesUnordered so the handshakes race — matching the real
    // attack shape, where an attacker opens connections in parallel
    // rather than strictly in series.
    let mut pending = FuturesUnordered::new();
    for _ in 0..STORM_SIZE {
        let addr = addr;
        let state = state.clone();
        let client_id_str = client_id_str.clone();
        pending.push(async move { open_authenticated_ws(addr, &state, &client_id_str).await });
    }
    let mut sockets: Vec<WsStream> = Vec::with_capacity(STORM_SIZE);
    let mut handshake_failures = 0usize;
    while let Some(result) = pending.next().await {
        match result {
            Ok(ws) => sockets.push(ws),
            Err(_) => handshake_failures += 1,
        }
    }

    // Brief settle so any eviction policy has a chance to fire.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let live = live_connections_for(&state, client_id).await;
    assert!(
        live <= PER_CLIENT_CONNECTION_CAP,
        "server must bound live connections per client_id to {PER_CLIENT_CONNECTION_CAP}, \
         but registered {live} live for one client_id after a {STORM_SIZE}-socket storm \
         ({} handshakes succeeded, {handshake_failures} failed) — at scale, this primitive \
         drives the server-side fan-out memory growth described in jaz0-a803",
        sockets.len(),
    );
}

/// FIX-CONTRACT: when the per-client cap is reached, eviction must drop
/// the OLDEST socket, not reject the newest. This is a UX correctness
/// requirement for the chosen mitigation — not an exploit on its own —
/// so that a benign client reconnecting after a network blip is never
/// locked out by its own stale sockets.
///
/// Today: no cap exists, so the test fails simply because no eviction
/// happens. After the fix, the (cap + 1)-th handshake must succeed AND
/// the first socket opened must be closed by the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eviction_policy_drops_oldest_socket_not_newest() {
    let state = make_test_state().await;
    let addr = start_test_server(state.clone()).await;
    let client_id_str = ClientId::new().to_string();

    // Open cap-many sockets, strictly in order so server-side
    // registration is ordered too.
    let mut sockets = Vec::with_capacity(PER_CLIENT_CONNECTION_CAP);
    for i in 0..PER_CLIENT_CONNECTION_CAP {
        let ws = open_authenticated_ws(addr, &state, &client_id_str)
            .await
            .unwrap_or_else(|e| panic!("handshake {i} failed: {e}"));
        sockets.push(ws);
    }

    // The (cap + 1)-th handshake represents a reconnect that comes in
    // while the older sockets are still registered. It must succeed.
    let _newest = open_authenticated_ws(addr, &state, &client_id_str)
        .await
        .expect("reconnect must succeed once the cap is reached (evict-oldest, not reject-newest)");

    // The oldest socket must now be closed by the server. We tolerate
    // either an explicit Close frame, an end-of-stream, or a transport
    // error — any of those signals that the server severed it.
    let oldest = &mut sockets[0];
    let outcome = tokio::time::timeout(Duration::from_secs(3), oldest.next()).await;
    let closed_by_server = matches!(
        outcome,
        Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None)
    );
    assert!(
        closed_by_server,
        "oldest socket must be closed by the server when the per-client cap is exceeded; \
         observed: {outcome:?}",
    );
}

/// EXPLOIT-PRIMITIVE: at sub-OOM scale, demonstrate that the server
/// holds pre-handshake sockets open indefinitely. Each parked upgrade
/// pins a Tokio task awaiting `socket.recv()`, an fd, and a recv
/// buffer. At larger scale this is the unauthenticated slowloris
/// pattern described on the ticket.
///
/// Today: all `STORM_SIZE` upgrades stay parked past the test's outer
/// deadline. The handshake timeout must cause all of them to be closed
/// within a bounded window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_ws_upgrades_are_not_held_open_indefinitely() {
    let state = make_test_state().await;
    let addr = start_test_server(state.clone()).await;
    let url = format!("ws://{addr}/apps/{}/ws", state.app_id);

    // Open many concurrent WS upgrades. None send a handshake frame;
    // each just parks on its socket waiting for the server to give up.
    let mut sockets: Vec<WsStream> = Vec::with_capacity(STORM_SIZE);
    for _ in 0..STORM_SIZE {
        let (ws, _) = connect_async(&url).await.expect("ws upgrade");
        sockets.push(ws);
    }

    // Wait long enough for a production handshake timeout (~10s) to
    // fire on every socket, plus margin. Then poll each one in parallel:
    // a closed-by-server socket returns a Close frame, end-of-stream,
    // or a transport error within a short window. A parked socket pends.
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut polls = FuturesUnordered::new();
    for mut ws in sockets {
        polls.push(async move { tokio::time::timeout(Duration::from_secs(2), ws.next()).await });
    }
    let mut still_parked = 0usize;
    while let Some(outcome) = polls.next().await {
        let closed_by_server = matches!(
            outcome,
            Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None)
        );
        if !closed_by_server {
            still_parked += 1;
        }
    }

    assert_eq!(
        still_parked, 0,
        "server must close pre-handshake sockets within the handshake timeout, \
         but {still_parked}/{STORM_SIZE} idle upgrades were still parked after 15s \
         — at scale, this primitive is the slowloris pattern described in jaz0-a803",
    );
}

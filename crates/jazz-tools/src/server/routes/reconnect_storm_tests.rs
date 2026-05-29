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
//!    against its own stale sockets. Evicted clients receive a
//!    `RateLimited` error frame followed by a policy close so the
//!    eviction is observable application-side, not just at the TCP layer.
//! 2. **Handshake-stage read timeout**: a WS upgrade that never sends
//!    an `AuthHandshake` frame cannot tie up server resources indefinitely.
//!
//! ## What these tests prove (and what they don't)
//!
//! The "concurrent storm" and "idle upgrade" tests are
//! **exploit-primitive demonstrations at sub-OOM scale**. They show that
//! without the mitigations the server accepts the relevant input pattern
//! (many concurrent sockets under one `client_id`; a parked WS upgrade
//! that never handshakes) without applying any bound. They are
//! intentionally small enough to run quickly in CI — they prove the
//! primitive exists, not that it crashes a production host. Severity at
//! scale comes from the analysis on the ticket, not from these tests.
//!
//! The remaining tests are **fix-shape contracts**. They pin properties
//! the mitigation must satisfy: oldest is the eviction target; other
//! `client_id`s are unaffected; repeated eviction keeps live count
//! bounded; the evicted client gets a `RateLimited` signal; the handshake
//! timeout interacts cleanly with shutdown.
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

use crate::jazz_transport::{ErrorCode, ServerEvent};
use crate::middleware::AuthConfig;
use crate::schema_manager::AppId;
use crate::server::routes::websocket::HANDSHAKE_READ_TIMEOUT;
use crate::server::{PER_CLIENT_CONNECTION_CAP, ServerBuilder, ServerState, StorageBackend};
use crate::sync_manager::ClientId;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Scale used by the exploit-primitive demonstrations. Big enough that the
/// failure message ("the server allowed N connections under one client_id"
/// / "the server kept N idle upgrades parked") reads as a real exploit
/// demonstration; small enough to run quickly inside CI's `ulimit -n`.
const STORM_SIZE: usize = 100;

/// Upper bound on how long a behaviour assertion polls before failing.
/// Generous enough to absorb scheduler jitter on loaded CI runners,
/// without making genuine failures slow.
const SETTLE_DEADLINE: Duration = Duration::from_secs(5);

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

/// Poll the live connection count for `client_id` until it satisfies
/// `predicate`, with a `SETTLE_DEADLINE` upper bound. Returns the last
/// observed count. Used so tests don't need to guess a fixed settle
/// window — flaky on loaded CI runners, especially under storms where
/// evicted tasks may still be in `ensure_client_*` or mid-`socket.send`
/// when a fixed-sleep test wakes.
async fn wait_for_live_count(
    state: &Arc<ServerState>,
    client_id: ClientId,
    predicate: impl Fn(usize) -> bool,
) -> usize {
    let start = tokio::time::Instant::now();
    let mut live = live_connections_for(state, client_id).await;
    while !predicate(live) && start.elapsed() < SETTLE_DEADLINE {
        tokio::time::sleep(Duration::from_millis(25)).await;
        live = live_connections_for(state, client_id).await;
    }
    live
}

/// Decode a post-handshake `ServerEvent` from a binary WS message.
/// Returns `None` if the message isn't a decodable event.
fn decode_post_handshake_event(msg: &WsMessage) -> Option<ServerEvent> {
    let WsMessage::Binary(bytes) = msg else {
        return None;
    };
    ServerEvent::decode_frame(bytes)
}

/// EXPLOIT-PRIMITIVE: at sub-OOM scale, demonstrate that the server has no
/// bound on concurrent connections per `client_id`. A single client_id
/// can register `STORM_SIZE` fan-out targets in `ConnectionEventHub`,
/// each with its own unbounded outbound channel — payload memory grows
/// as `connections × per-channel backlog`, and the same primitive at
/// larger scale OOMs the server (see ticket analysis).
///
/// Today's gap: without the cap, all `STORM_SIZE` sockets register. The
/// cap must reduce the live count to at most `PER_CLIENT_CONNECTION_CAP`,
/// AND it must do so via eviction — none of the handshakes are rejected.
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

    assert_eq!(
        handshake_failures, 0,
        "the cap must evict-not-reject: all {STORM_SIZE} handshakes should succeed",
    );

    let live = wait_for_live_count(&state, client_id, |c| c <= PER_CLIENT_CONNECTION_CAP).await;
    assert!(
        live <= PER_CLIENT_CONNECTION_CAP,
        "server must bound live connections per client_id to {PER_CLIENT_CONNECTION_CAP}, \
         but {live} were still live for one client_id after a {STORM_SIZE}-socket storm \
         and a {SETTLE_DEADLINE:?} settle window — at scale, this primitive drives the \
         server-side fan-out memory growth described in jaz0-a803",
    );
}

/// FIX-CONTRACT: when the per-client cap is reached, eviction must drop
/// the OLDEST socket, not reject the newest. This is a UX correctness
/// requirement for the chosen mitigation — not an exploit on its own —
/// so that a benign client reconnecting after a network blip is never
/// locked out by its own stale sockets.
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
    // error — any of those signals that the server severed it. The
    // intervening RateLimited error frame is checked by a dedicated test.
    let oldest = &mut sockets[0];
    let outcome = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match oldest.next().await {
                Some(Ok(WsMessage::Binary(_))) => continue, // skip the RateLimited error frame
                other => break other,
            }
        }
    })
    .await;
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

/// FIX-CONTRACT: an evicted connection must receive a programmatic signal
/// before the close frame, so clients can distinguish cap eviction from
/// a generic TCP-level disconnect. We assert the `RateLimited` error
/// event arrives, followed by a policy close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evicted_connection_receives_rate_limited_error_then_close() {
    let state = make_test_state().await;
    let addr = start_test_server(state.clone()).await;
    let client_id_str = ClientId::new().to_string();

    let mut oldest = open_authenticated_ws(addr, &state, &client_id_str)
        .await
        .expect("open oldest handshake");
    // Fill the cap with placeholder sockets we don't read from.
    let mut _fillers = Vec::with_capacity(PER_CLIENT_CONNECTION_CAP);
    for _ in 0..PER_CLIENT_CONNECTION_CAP {
        _fillers.push(
            open_authenticated_ws(addr, &state, &client_id_str)
                .await
                .expect("filler handshake"),
        );
    }

    let mut saw_rate_limited = false;
    let mut saw_close = false;
    let mut close_code = None;
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(msg) = oldest.next().await {
            match msg {
                Ok(msg) => {
                    if let Some(event) = decode_post_handshake_event(&msg) {
                        if let ServerEvent::Error { code, .. } = event
                            && code == ErrorCode::RateLimited
                        {
                            saw_rate_limited = true;
                        }
                    }
                    if let WsMessage::Close(frame) = msg {
                        saw_close = true;
                        close_code = frame.map(|f| f.code);
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await;

    assert!(
        saw_rate_limited,
        "evicted socket must receive a ServerEvent::Error with code RateLimited before close",
    );
    assert!(
        saw_close,
        "evicted socket must receive an application-level Close frame"
    );
    assert_eq!(
        close_code,
        Some(tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy),
        "close code must be Policy (1008) for cap-driven evictions",
    );
}

/// FIX-CONTRACT: eviction is scoped to the offending `client_id`.
/// A storm against client X must not touch client Y's connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eviction_does_not_affect_other_client_ids() {
    let state = make_test_state().await;
    let addr = start_test_server(state.clone()).await;
    let client_x_str = ClientId::new().to_string();
    let client_y_str = ClientId::new().to_string();
    let client_x = ClientId::parse(&client_x_str).expect("parse x");
    let client_y = ClientId::parse(&client_y_str).expect("parse y");

    // Open Y's connections first, well under the cap, and hold them.
    let mut _y_sockets = Vec::with_capacity(PER_CLIENT_CONNECTION_CAP);
    for _ in 0..PER_CLIENT_CONNECTION_CAP {
        _y_sockets.push(
            open_authenticated_ws(addr, &state, &client_y_str)
                .await
                .expect("open Y socket"),
        );
    }
    let y_baseline = live_connections_for(&state, client_y).await;
    assert_eq!(
        y_baseline, PER_CLIENT_CONNECTION_CAP,
        "Y should have exactly cap-many connections established",
    );

    // Storm X concurrently with Y already at the cap.
    let mut pending = FuturesUnordered::new();
    for _ in 0..STORM_SIZE {
        let addr = addr;
        let state = state.clone();
        let client_x_str = client_x_str.clone();
        pending.push(async move { open_authenticated_ws(addr, &state, &client_x_str).await });
    }
    let mut _x_sockets: Vec<WsStream> = Vec::with_capacity(STORM_SIZE);
    while let Some(result) = pending.next().await {
        if let Ok(ws) = result {
            _x_sockets.push(ws);
        }
    }

    let x_live = wait_for_live_count(&state, client_x, |c| c <= PER_CLIENT_CONNECTION_CAP).await;
    assert!(
        x_live <= PER_CLIENT_CONNECTION_CAP,
        "X live count must be bounded by cap; got {x_live}",
    );

    let y_live = live_connections_for(&state, client_y).await;
    assert_eq!(
        y_live, PER_CLIENT_CONNECTION_CAP,
        "Y connections must not be evicted by X's storm; expected {PER_CLIENT_CONNECTION_CAP}, got {y_live}",
    );
}

/// FIX-CONTRACT: a steady stream of reconnects (each evicting the
/// previous oldest) keeps the live count at the cap rather than letting
/// it drift up over time. This guards against an off-by-one in the
/// eviction count or a state-machine regression that only evicts on the
/// first overflow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_evictions_keep_live_count_at_cap() {
    let state = make_test_state().await;
    let addr = start_test_server(state.clone()).await;
    let client_id_str = ClientId::new().to_string();
    let client_id = ClientId::parse(&client_id_str).expect("parse client_id");

    // Fill to the cap.
    let mut sockets = Vec::with_capacity(PER_CLIENT_CONNECTION_CAP);
    for _ in 0..PER_CLIENT_CONNECTION_CAP {
        sockets.push(
            open_authenticated_ws(addr, &state, &client_id_str)
                .await
                .expect("fill handshake"),
        );
    }
    let initial = wait_for_live_count(&state, client_id, |c| c == PER_CLIENT_CONNECTION_CAP).await;
    assert_eq!(initial, PER_CLIENT_CONNECTION_CAP);

    // Cycle: open one new connection, then check the live count.
    // Each new socket must evict exactly one old socket, keeping the
    // total live count at the cap.
    for cycle in 0..(PER_CLIENT_CONNECTION_CAP * 3) {
        let _new = open_authenticated_ws(addr, &state, &client_id_str)
            .await
            .unwrap_or_else(|e| panic!("cycle {cycle} reconnect failed: {e}"));

        let live = wait_for_live_count(&state, client_id, |c| c == PER_CLIENT_CONNECTION_CAP).await;
        assert_eq!(
            live, PER_CLIENT_CONNECTION_CAP,
            "live count must stay at cap after cycle {cycle}; observed {live}",
        );
        sockets.push(_new);
    }
}

/// EXPLOIT-PRIMITIVE: at sub-OOM scale, demonstrate that the server
/// holds pre-handshake sockets open indefinitely. Each parked upgrade
/// pins a Tokio task awaiting `socket.recv()`, an fd, and a recv
/// buffer. At larger scale this is the unauthenticated slowloris
/// pattern described on the ticket.
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

    // Wait `HANDSHAKE_READ_TIMEOUT` plus a margin for the timer to fire
    // and the close frames to land. Then poll each one in parallel.
    tokio::time::sleep(HANDSHAKE_READ_TIMEOUT + Duration::from_secs(2)).await;

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
         but {still_parked}/{STORM_SIZE} idle upgrades were still parked after \
         {HANDSHAKE_READ_TIMEOUT:?} + 2s — at scale, this primitive is the slowloris \
         pattern described in jaz0-a803",
    );
}

/// FIX-CONTRACT: a parked pre-handshake socket must close cleanly when
/// the server is shutting down, with no panic and no resource leak,
/// regardless of which branch of the handshake select wins the race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_ws_upgrade_during_shutdown_closes_cleanly() {
    let state = make_test_state().await;
    let addr = start_test_server(state.clone()).await;
    let url = format!("ws://{addr}/apps/{}/ws", state.app_id);
    let (mut ws, _) = connect_async(&url).await.expect("ws upgrade");

    // Give the server a moment to enter the handshake-wait select,
    // then request shutdown. Shutdown should win the race against the
    // 10s handshake timer.
    tokio::time::sleep(Duration::from_millis(100)).await;
    state.shutdown.request_shutdown();

    let outcome = tokio::time::timeout(Duration::from_secs(3), ws.next()).await;
    let closed_by_server = matches!(
        outcome,
        Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None)
    );
    assert!(
        closed_by_server,
        "parked pre-handshake socket must close cleanly under shutdown; observed: {outcome:?}",
    );
}

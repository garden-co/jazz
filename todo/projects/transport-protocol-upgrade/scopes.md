# Scopes

Detailed architecture and flow definitions: [WebSocket Transport Spec](../../specs/todo/a_mvp/websocket_transport.md)

---

## 1. Core transport abstraction — `jazz-tools` crate

The foundational scope. Define `StreamAdapter`, `TickNotifier`, `TransportHandle`, `TransportManager` in core Rust. Remove the `SyncSender` generic from `RuntimeCore`.

- [ ] Define `StreamAdapter` trait: `connect()`, `send()`, `recv()`, `close()`
- [ ] Define `TickNotifier` trait: `notify()` — signals scheduler to run `batched_tick()`
- [ ] Implement `TransportHandle` (two channel endpoints: `outbox_tx` + `inbound_rx`)
- [ ] Implement `TransportManager<W: StreamAdapter, T: TickNotifier>`: send loop (outbox channel → ws), recv loop (ws → inbound channel + tick notify), reconnection (exponential backoff, jitter), auth handshake (first message after connect)
- [ ] Implement `NativeWsStream` adapter using `tokio-tungstenite` (~30 LOC) — shared by NAPI, React Native, server, and tests
- [ ] Implement `WasmWsStream` adapter using `web-sys::WebSocket` / `ws_stream_wasm` (~30 LOC) — browser WASM only
- [ ] Remove `SyncSender` trait and generic parameter from `RuntimeCore`
- [ ] Update `batched_tick()`: drain `inbound_rx` → park messages, drain outbox → push to `outbox_tx`
- [ ] Add `set_transport()` / `clear_transport()` methods on `RuntimeCore`
- [ ] Update test helpers: replace `VecSyncSender` with test `TransportHandle` backed by channel inspection

## 2. WebSocket server endpoint — replace SSE + POST

Replace the server's `GET /events` and `POST /sync` with a single `/ws` WebSocket upgrade endpoint.

- [ ] Add `/ws` WebSocket upgrade endpoint via axum's built-in support
- [ ] Implement bidirectional message handler: receive `SyncBatchRequest` frames, send `ServerEvent` frames on same connection
- [ ] Auth handshake: read first message after upgrade, authenticate using existing `middleware/auth.rs` logic
- [ ] Replace broadcast channel with per-connection `mpsc` channel for server→client events
- [ ] Connection lifecycle: track connections, disconnect candidates with TTL, `client_id` reconnect semantics
- [ ] Heartbeat via WebSocket ping/pong frames (replace 30s SSE heartbeat)
- [ ] Delete `GET /events` SSE route, SSE binary frame encoder, `POST /sync` handler from `routes.rs`

## 3. Platform integration — wire `connect()` / `disconnect()` across all targets

Each platform crate gets `connect()` / `disconnect()` and a `TickNotifier` impl (~5 LOC). Delete old `SyncSender` impls and JS callback machinery.

- [ ] **jazz-wasm**: `WasmTickNotifier` (clones `WasmScheduler`, calls `schedule_batched_tick()`). `connect()` spawns `TransportManager<WasmWsStream, WasmTickNotifier>` via `spawn_local`. Delete `JsSyncSender`. Keep `onSyncMessageReceived()` / `onSyncMessageToSend()` for worker bridge only.
- [ ] **jazz-napi**: `NativeTickNotifier` (clones `NapiScheduler`, calls via TSFN). `connect()` spawns via `tokio::spawn`. Delete `NapiSyncSender`, `onSyncMessageToSend()`, `onSyncMessageReceived()`.
- [ ] **jazz-rn**: same `NativeTickNotifier` (clones `RnScheduler`, calls via UniFFI callback). `connect()` spawns on background thread with tokio runtime. Delete `RnSyncSender`, `onSyncMessageToSend()`, `onSyncMessageReceived()`.
- [ ] **TypeScript**: delete network-facing code in `sync-transport.ts` (`StreamController`, `sendSyncPayloadBatch()`, `readBinaryFrames()`). Update `client.ts` and `jazz-worker.ts` to call `runtime.connect(url, authJson)`.
- [ ] **Rust client** (`transport.rs`): same `NativeTickNotifier` + `NativeWsStream`. Delete `reqwest`-based transport.

## 4. Ordering tests — un-ignore and validate

The north-star deliverable. No new transport code — validates the new architecture fixes the ordering bugs.

- [ ] Un-ignore `subscription_reflects_final_state_after_rapid_bulk_updates` — 500 rapid writes must arrive in order
- [ ] Un-ignore `single_client_operations_reach_server_in_causal_order` — ownership transfer + write must land in causal order
- [ ] New test: `concurrent_writers_maintain_per_client_order` — Alice and Bob write rapidly; each client's writes arrive in authored order
- [ ] New test: reconnection ordering — disconnect mid-burst, verify buffered messages drain in order after reconnect
- [ ] Verify all existing sync/subscribe integration tests pass

## 5. WebTransport adapter (future, deferrable) — drop-in upgrade

Add `StreamAdapter` impls for WebTransport. Same `TransportManager`, same `TickNotifier`, same `TransportHandle`. Just a different stream underneath.

- [ ] `NativeWtStream` implementing `StreamAdapter` (Rust server + client) using [`wtransport`](https://github.com/BiagioFesta/wtransport) — single bidirectional stream
- [ ] `WasmWtStream` implementing `StreamAdapter` (browser) using browser `WebTransport` API via `web-sys`
- [ ] Client negotiation: try WebTransport → fall back to WebSocket on failure/timeout
- [ ] Server: QUIC listener alongside TCP
- [ ] TLS certificate handling for QUIC
- [ ] AWS infra: NLB `TCP_QUIC` listener
- [ ] Parameterize test suite over adapter type — all tests must pass on both

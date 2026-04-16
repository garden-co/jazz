---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Replace HTTP `/sync` + SSE `/events` with a single Rust-owned WebSocket `/ws` transport, and wire JWT rotation and server-side auth rejection end-to-end across all bindings.

**Transport rewrite**

- New `TransportManager` in `jazz-tools` owns the WebSocket connection, framing (4-byte big-endian length + JSON), reconnect with exponential backoff, periodic heartbeat, and a `TransportControl` channel that observes `Shutdown` / `UpdateAuth` in every phase (connect, backoff, handshake, connected). Dropped `TransportHandle` triggers an implicit shutdown.
- New `install_transport` helper on `RuntimeCore` centralises the boilerplate (create manager → seed catalogue state hash → register handle → spawn) so all four bindings converge on one code path.
- `NativeWsStream` (tokio-tungstenite + rustls) and `WasmWsStream` (ws_stream_wasm) implement the shared `StreamAdapter` trait.

**Auth refresh**

- `JazzClient.updateAuthToken(jwt)` now pushes the refreshed credentials into the live transport via `runtime.updateAuth`, which routes through `TransportControl::UpdateAuth` and triggers a reconnect with the new auth. Previously the call only mutated local context.
- `ConnectSyncRuntimeOptions.onAuthFailure` is now wired to `runtime.onAuthFailure` and fires whenever the server rejects the WS handshake with `Unauthorized`. NAPI exposes a `ThreadsafeFunction`-based callback; React Native exposes a UniFFI `callback_interface`; WASM keeps its existing `Function` callback.
- The worker posts `auth-failed` back to the main thread when `runtime.updateAuth` throws, and supports `update-auth` / `disconnect-upstream` / `reconnect-upstream` messages from the bridge.

**Bindings**

- WASM, NAPI, and React Native all now expose `connect`, `disconnect`, `update_auth`, `on_auth_failure`.
- React Native: `JazzRnRuntimeAdapter` forwards `updateAuth` and `onAuthFailure` to the UniFFI binding (previously missing — auth refresh was a silent no-op on RN).
- `JazzClient.updateAuthToken` carries `admin_secret` and `backend_secret` forward from context (previously the serialised payload only included `jwt_token`, silently erasing privileged credentials on every refresh).

**Breaking changes**

- `POST /sync` and `GET /events` HTTP routes are deleted; external callers receive 404. Use the WebSocket `/ws` route via `runtime.connect(url, authJson)`.
- `RuntimeCore<S, Sch, Sy>` is now `RuntimeCore<S, Sch>` — the `SyncSender` generic parameter has been removed.
- `NapiSyncSender` and `RnSyncSender` are removed; bindings use `runtime.connect` instead.
- `TokioRuntime::new` no longer takes the trailing `SyncSender` argument.
- Cargo features `transport` and `transport-http` are removed; transport types are default-on, and `transport-websocket` enables the WS implementation.

**Tests**

- Inline `TransportManager` tests cover shutdown and `update_auth` in every phase (connect / backoff / handshake / connected).
- Re-enabled two previously-`#[ignore]`d sync-reliability tests after fixing a debounce-flag race in `TokioScheduler::schedule_batched_tick`.
- Integration tests migrated from `/events` to `/ws`.
- New TS coverage: `client.test.ts` (onAuthFailure wiring + secret preservation), `napi.auth-failure.test.ts` (E2E against real NAPI + server), `db.transport.test.ts`, `url.test.ts`, `db.auth-refresh.worker.test.ts` (browser worker round-trip), RN adapter tests for `updateAuth` / `onAuthFailure`.

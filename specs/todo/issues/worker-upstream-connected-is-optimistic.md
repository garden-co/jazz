# Upstream-connected signalling is optimistic (worker and direct paths)

## What

The bridge/runtime signal that unblocks edge/global-tier queries fires as soon as `runtime.connect()` returns, not when the Rust WebSocket actually opens. Edge queries unblock while the WS is still handshaking or unreachable — they then fail at the Rust/WS layer (or silently resolve against stale local state) instead of waiting.

Affects two call sites:

- **Worker path** — `packages/jazz-tools/src/worker/jazz-worker.ts:251-263` (`performUpstreamConnect`) posts `upstream-connected` immediately after `runtime.connect?.()` returns.
- **Direct (non-worker) path** — `packages/jazz-tools/src/runtime/db.ts:591-595`. Direct clients with a `serverUrl` now default `defaultDurabilityTier` to `"edge"` (db.ts:567-572) and kick off `client.connectTransport(...)` synchronously, but there is no wait gate at all. A one-shot `db.query()` / `db.all()` issued during startup can resolve from local state before the WS handshake finishes, because `QueryManager` treats `!self.sync_manager.has_servers()` as "frontier complete" (`crates/jazz-tools/src/query_manager/subscriptions.rs:127-129`).

## Priority

medium

## Notes

- `runtime.connect()` is sync-return-void and does not expose WS open/close callbacks to TS.
- Real fix requires a Rust-side hook: either an `onConnectStateChanged` callback on the runtime, or an awaitable `connect()` that resolves on WS open. Once that exists, both call sites can gate their "upstream live" signal on it.
- Matching signal on the other side (disconnect) is also missing — Rust auto-reconnect is silent to TS, so a later drop won't flip the bridge/runtime back to "waiting". Worker currently only posts `upstream-disconnected` on auth failure (jazz-worker.ts:313-315) or explicit manual disconnect (:513-514); ordinary connect failures and reconnect drops go unreported.
- Direct-path symptom is worse than worker-path: worker at least has a bridge gate to miss; direct path has no gate at all and will happily resolve stale local reads as if they were frontier-complete.

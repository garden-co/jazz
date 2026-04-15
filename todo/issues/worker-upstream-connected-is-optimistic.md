# Worker posts upstream-connected optimistically

## What

The worker posts `upstream-connected` to the main-thread bridge as soon as `runtime.connect()` returns, not when the Rust WebSocket actually opens. Edge/global-tier queries unblock even if the WS is still handshaking or the server is unreachable — they'll then fail at the Rust/WS layer instead of waiting.

## Priority

medium

## Notes

- Introduced alongside the fix for the previous "worker never posts upstream-connected" bug (jazz-worker.ts `performUpstreamConnect`).
- `runtime.connect()` is sync-return-void and does not expose WS open/close callbacks to TS.
- Real fix requires a Rust-side hook: either an `onConnectStateChanged` callback on the runtime, or an awaitable `connect()` that resolves on WS open.
- Matching signal on the other side (disconnect) is also missing — Rust auto-reconnect is silent to TS, so a later drop won't flip the bridge back to "waiting".

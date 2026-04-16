# WASM TransportManager backoff has no real sleep

## What

`ReconnectState::backoff` in `crates/jazz-tools/src/transport_manager.rs:161-179` awaits `futures::future::ready(())` on the `wasm32` branch instead of a real timer. If `W::connect` fails synchronously (bad URL, immediate TLS reject, offline), the outer reconnect loop busy-spins with no backpressure — a real reconnect-storm risk on mobile Safari/Firefox when connectivity is flaky.

## Priority

high

## Notes

- Tokio path uses `tokio::time::sleep` correctly; only the `#[cfg(any(target_arch = "wasm32", not(feature = "runtime-tokio")))]` branch is broken.
- The computed `delay_ms` is explicitly discarded (`let _ = delay_ms;`).
- Fix: route through a `gloo-timers`-backed sleep or through the scheduler via `wasm-bindgen-futures` / `setTimeout`.
- Comment claims "rely on network I/O awaits for real backpressure" — that assumption fails for synchronous connect errors.

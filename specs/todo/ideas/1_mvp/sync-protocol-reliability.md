# Sync Protocol Reliability & Unification

## What

Fix critical reliability gaps in the sync path and unify the transport layer across network sync (client-server), worker communication (main thread-worker), and peer replication (server-server).

## Notes

- A local change can look successful but never reach other devices. Lost messages can poison later incremental sync, and reconnect currently repairs receive paths better than send paths.
- Network sync uses HTTP POST + SSE while the worker bridge uses `postMessage` with typed arrays, which creates duplicated framing, parsing, and state-machine logic.
- This affects all Jazz consumers because it impacts durability, consistency, and transport-layer correctness.

### Reliability gaps

Six gaps identified: (1) outbound messages can arrive out of order (one async task per payload), (2) outbox drained before delivery confirmed, (3) lost message poisons later incremental sync, (4) server returns per-message results but client ignores response body, (5) reconnect repairs receive side better than send side, (6) data and control messages share the same fragile path. There's an ignored regression test: `subscription_reflects_final_state_after_rapid_bulk_updates`.

### Protocol unification

Open questions: WebSocket vs HTTP/2 vs both as transports with shared framing, message framing format, backpressure/flow control, compression strategy, interaction with existing SSE path.

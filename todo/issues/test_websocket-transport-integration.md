# Missing integration test suite for WebSocket transport

## What

No `crates/jazz-tools/tests/websocket_transport.rs` exists. The inline `#[cfg(test)]` module in `transport_manager.rs` covers the run-loop state machine but not end-to-end behaviour against a real `/ws` server.

## Priority

medium

## Notes

Scenarios to cover:

- Concurrent writers: two clients both push sync payloads through `/ws` simultaneously, server fans out correctly.
- Reconnect resume: client loses connection mid-stream, reconnects with a catalogue_state_hash, receives missing deltas without duplication.
- Auth failure round-trip: server rejects handshake, client transitions to parked state, `update_auth` unparks and reconnects with fresh credentials.
- Graceful close: client disconnect emits clean WS close frame; server cleans up the connection in `ConnectionEventHub`.

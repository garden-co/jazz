# PostMessageStream adapter to unify worker bridge under StreamAdapter

## What

The browser main-thread ↔ worker bridge currently uses a bespoke `postMessage` path and the `JsSyncSender` Rust-side shim. A `PostMessageStream` adapter implementing `StreamAdapter` would let the worker bridge reuse the same `TransportManager` + run-loop code path as the network transport, eliminating the `sync_sender` field and `JsSyncSender` entirely.

## Priority

low

## Notes

- Payoff: one transport model across network and worker; worker bridge gets free reconnect/backoff/auth-failure handling.
- Complication: the worker bridge is bidirectional `postMessage`, not a WebSocket. Adapter has to translate `Uint8Array` batches into the length-prefixed frame format the manager expects — doable, but the semantics (no real "disconnect") need thought.
- Deferred follow-up from the auth-refresh / transport-rewrite PR.

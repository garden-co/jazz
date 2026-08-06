# Db disconnect/reconnect restore

## Restored surface

- `Db.disconnect(): Promise<void>` is public again.
- `Db.reconnect(): Promise<void>` is public again.
- `disconnect()` marks the `Db` as intentionally offline, disconnects every existing schema client from its server transport, and leaves the local runtime/store alive.
- `reconnect()` clears the offline marker and reconnects every existing schema client to the configured `serverUrl` with the current auth config.
- Schema clients created while the `Db` is intentionally disconnected are kept offline until `reconnect()` is called.

## Runtime notes

- The implementation routes through the existing runtime `connect`/`disconnect` transport methods.
- The persistent browser worker `disconnect()` path is now awaitable; `Db.disconnect()` waits for the worker `disconnect` RPC instead of resolving while the RPC is still in flight.
- The native runtime adapter pumps the server transport and refreshes open plain subscriptions once the replacement carrier is ready after reconnect.

## Test notes

- Restored the historical browser coverage at `packages/jazz-tools/tests/browser/db.disconnect.test.ts`, ported to the current browser test helpers.
- Added focused node-level API wiring coverage in `packages/jazz-tools/src/runtime/db.transport.test.ts`:
  - existing clients receive public `disconnect()`/`reconnect()` lifecycle calls;
  - clients first created while disconnected do not connect until `reconnect()`.

## Intentionally-offline local-only read contract

While a `Db` is intentionally offline, a read with
`{ tier: "local", localUpdates: "immediate", propagation: "local-only" }`
resolves from the current local materialized state. It does not wait for an
upstream coverage frontier and it does not inspect the server:

- a locally committed, pending write is returned immediately;
- a row written remotely while the client is offline is absent (an empty result
  for a query matching only that row) until reconnect delivery reaches the
  local store.

`propagation: "local-only"` chooses the local snapshot; it is not a request to
wait until that snapshot becomes complete relative to an unavailable upstream.
The browser regression asserts this for both direct-memory and persistent-worker
runtimes, then separately asserts convergence after `reconnect()`.

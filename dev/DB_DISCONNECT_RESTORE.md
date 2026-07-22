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

## Current browser findings

The restored browser test is runnable in this worktree but still fails after the public API/lifecycle restore:

- direct memory mode local reads after a disconnected write reject with `invalid offset`;
- direct memory mode local-only reads for missed remote writes now resolve immediately instead of staying pending under the old pre-swap semantics;
- persistent worker mode also hits `invalid offset` when reading after a disconnected local write;
- persistent worker mode does not observe the missed remote update within the restored test timeout after reconnect.

I left the browser test in place as the recovered regression target rather than weakening its expected semantics. The focused runtime test and `tsc --noEmit` pass for the restored public API.

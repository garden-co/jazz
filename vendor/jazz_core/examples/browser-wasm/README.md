# TS WASM Browser Example

This package is a Vite scaffold for importing the `jazz-wasm/pkg-web`
web-target build in a browser worker and driving direct `WasmDb` objects from
the main thread.

The page starts `src/db-worker.ts`, wraps it with the local
`BrowserWasmWorkerClient`, and sends direct `WasmDb` operations to the
worker. The worker initializes the generated WASM module, owns the live
`WasmDb`/transport objects, revives transferred bytes, and executes the small
allow-listed browser surface.

The main thread owns the demo flow, handle bookkeeping, status UI, and decoded
row rendering, while the worker owns the WASM module instance and `WasmDb` objects. The
browser-owned boundary is deliberately thin so the example stays close to the
direct object model while the browser integration is still settling.

The demo runs a browser worker-backed memory database scenario:

- open a memory DB
- define a `todos` schema with `title` and `done`
- prepare a `todos` table query
- open a subscription with `WasmDb.subscribe`
- insert `Ship direct WasmDb` with Record-encoded row/cell bytes
- update the todo through `WasmDb.updateEncoded`
- assert identity-scoped update dry-runs through `dbCanUpdateEncodedForIdentity`
  over the worker boundary, allowing the owner and denying a different identity
- delete the todo through `WasmDb.delete`
- wait for local durability and render each watch state into DOM transition rows
- unsubscribe, close handles, and shut the worker down

The default tour still uses memory storage so it can keep exercising
insert/update/delete cleanup deterministically.
The browser reload-persistence smoke check opens OPFS through
`WasmDb.openBrowser(namespace, schema, config)`. The smoke writes a real Jazz
todo row, starts a fresh page/worker, reopens the same browser storage
namespace, and reads the row back from browser-owned durable bytes. The browser
storage concurrency smoke opens one browser storage namespace in a worker,
starts a second worker opening the same namespace while the first still holds
the OPFS access handle, then releases the first handle and asserts the second
`WasmDb.openBrowser(...)` completes through the retry path. The browser batch
durability smoke performs three sequential Record-encoded writes,
closes the worker-owned DB, reopens it in a fresh worker, reads all rows
back, then verifies a subscription opened after reopen sees a follow-up update.

The browser smoke check has two websocket modes. `?smoke=websocket-boundary`
opens the same worker-backed `WasmDb` shape, attaches an `upstream` transport,
writes an encoded todo row, drives `transportRecvWireFrame`, and uses a
recording browser `WebSocket` constructor to assert that opaque binary
byte batches would be sent to the sync URL.
`?smoke=websocket-rust&ws=...` uses the native browser `WebSocket` constructor
against a real Rust `jazz-server` listener spawned by the Playwright smoke
runner, then asserts that binary frames are sent and received through that
process boundary.

## Reload-persistence target

The reload-persistence target behavior is intentionally narrow: add a todo in a
local-first browser app, reload the browser, reopen local state, and render that
todo from durable browser-owned bytes. The positive smoke drives two query-param
page modes with the same unique namespace:

- `?smoke=reload-write&ns=...` creates `Survive reload`, waits for local
  durability, and closes the DB/storage after the write has flushed through
  `WasmDb.openBrowser(...)`.
- `?smoke=reload-verify&ns=...` starts a new worker, reopens the same
  browser storage namespace, subscribes to `todos`, asserts
  `Survive reload:open`, and removes the OPFS file for that namespace.
- `?smoke=browser-concurrency&ns=...` holds a browser storage namespace in one
  worker, starts a same-namespace open in a second worker, releases the first
  handle, asserts the second open succeeds, and removes the OPFS file.
- `?smoke=browser-batch-durability&ns=...` inserts three todos with
  Record-encoded row/cell bytes, waits for local durability for each write, closes and
  reopens the same browser storage namespace, reads the rows back, opens a
  watch after reopen, updates one row, and asserts the watch reflects the
  update before cleanup.

`?smoke=websocket-boundary` remains the cheap browser-side attach/drive/send
check. `npm run smoke:built` also runs `?smoke=websocket-rust&ws=...`: the
Playwright harness asks the already-built browser bundle for the exact todo
schema hex, starts `cargo run -q -p jazz-server -- serve-loopback-websocket-schema
<schema> --allow-legacy-query-identity true`, passes the printed `ws_url` to the
page, and verifies one browser client can exchange websocket byte batches with
that Rust listener. Full two-client convergence remains
covered by the sibling Node websocket smoke checks.

That storage path is still deliberately smaller than a complete browser
durability story. Remaining browser durability work includes broader
transactional batch behavior, durable history/index/table partition coverage,
cursor and subscription correctness after reload, ABI error mapping, format
versioning, quota/cleanup handling, and worker-safe concurrency.

Rows and cells stay Record-encoded across both the worker boundary and the WASM
boundary because this demo is exercising direct `WasmDb` bindings, not a
higher-level object mapper. The example only decodes rows after `WasmDb` returns
bytes; typed progress events then let the page render compact runtime,
durability, read, and watch facts plus the decoded todo states while preserving
the `#summary` and `#log` smoke anchors.

The browser demo owns its direct `WasmDb` browser glue locally. Domain-level helpers
for schemas, encoded cells, and row views come from the `jazz-tools` package
root; browser-specific query/config/event decoding stays inside this package.

## Scripts

- `npm run build:wasm` builds `../../jazz-wasm/pkg-web` with `wasm-pack --target web --release`.
- `npm run check` typechecks the browser TypeScript.
- `npm run build` builds the Vite app.
- `npm run dev` starts the browser scaffold locally.
- `npm run smoke:built` serves the existing Vite build with `vite preview`,
  opens it in headless Chromium, verifies the worker-backed WASM flow reaches
  `Ready`, checks reload persistence, checks browser storage worker handoff,
  checks browser batch durability after reopen, checks the recording websocket
  boundary, and spawns a real Rust websocket listener for the native browser
  WebSocket smoke.
- `npm run smoke` builds the Vite app, then runs `smoke:built`.
- `npm run demo:bundle` builds WASM, typechecks, and builds the Vite app.
- `npm run demo` runs `demo:bundle`, then runs the browser smoke against that build.
- `npm test` runs the full demo check.

Run it from this directory:

```sh
npm install
npm test
npm run dev
```

If Playwright reports that Chromium is missing on a fresh machine, run
`npx playwright install chromium` once and rerun `npm test` or `npm run smoke`.

The example imports `../../../jazz-wasm/pkg-web/jazz_wasm.js` directly so it
stays close to the generated web package. The Node examples use
`../../../jazz-wasm/pkg`, so browser and Node builds do not overwrite each
other. Rebuild WASM after changing `jazz-wasm`.

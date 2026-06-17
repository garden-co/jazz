# jazz-wasm: "memory access out of bounds" on rapid multi-client page teardown

## What

When a browser page hosting two or more Jazz clients (e.g. two `createJazzClient`
in one tab, or a leader/follower across Dbs sharing a `dbName`) is reloaded or
closed while sync is in flight, the Rust→WASM core traps with
`RuntimeError: memory access out of bounds` (release builds). It is
timing-dependent and fires in the page that is navigating away — and, in the
worker-backed setup, in the dedicated worker.

The trap is **inert**: it only fires during teardown of a page that is already
gone, so the corrupted heap dies with the page. There is no data loss and no
functional failure — verified by a baseline run of the `sveltekit-csr-ssr` e2e
with the suppressor removed: the OOB-prone reload-churn step
(`reloadUntilSectionContains`) never fails, with or without the fix. The only
user-visible effect was alarming console errors / uncaught errors during page
navigation.

## Root cause

Each client owns a `ws_stream_wasm` WebSocket transport driven by
`wasm_bindgen_futures::spawn_local(manager.run())`. On abrupt navigation the
browser force-closes every socket; `ws_stream_wasm`'s close/message callbacks
fire into a WASM linear memory that is being torn down concurrently, corrupting
the heap. The next code to touch the corrupted region traps — observed as either
an OOB inside the scheduler's `setTimeout(0)` once-task or a
`send_wrapper::invalid_deref` panic inside the WebSocket message callback.

Bisected facts:

- Needs ≥2 coexisting runtimes each with a transport. A single runtime, two
  without `connect()`, or two with a clean `shutdown()` never corrupt.
- ~one trap per page teardown; not reproducible without real page navigation
  (hence not in vitest).
- jazz's own Rust is safe (no `unsafe`); the corruption is in `ws_stream_wasm` /
  `wasm-bindgen-futures` unsafe code at the teardown boundary.

## Why it isn't fixed at the source

- The page is already navigating away. `WasmRuntime::disconnect()` only signals
  the transport manager over a channel; the `spawn_local` future is never polled
  again before navigation completes, so it cannot tear the socket down in time.
- `ws_stream_wasm` is already on its latest release (0.7.5) — no upstream bump.

## Mitigation (shipped)

Because the trap is inert, the runtime suppresses that one specific trap inside
the teardown window opened by `pagehide`
(`packages/jazz-tools/src/runtime/wasm-teardown-trap-suppressor.ts`): a `window`
error/rejection handler on the main thread, and a worker-scope handler gated on a
`__jazzWorkerTearingDown` flag the Rust host sets on the `pagehide` lifecycle
hint. A genuine out-of-bounds error during normal operation still surfaces.
Covered by `wasm-teardown-trap-suppressor.test.ts`.

## Known residuals

- Worker-origin console traps can still leak under heavy cold-start churn: the
  worker teardown flag is delivered by `postMessage` and can lose the race
  against abrupt worker termination. Cosmetic (the worker is dying).
- The corruption itself still happens; only its surfacing is suppressed.

## Priority

low — inert; cosmetic console noise only.

## Future true-fix options

- Synchronous raw-socket teardown: surface the `web_sys::WebSocket` from
  `WasmWsStream::connect` back to `WasmRuntime`, and on `disconnect()` /
  `pagehide` synchronously `ws.close()` + null `onmessage/onclose/onerror/onopen`
  so the force-close fires no callbacks into the dying heap.
- Fork/patch `ws_stream_wasm` to deregister its WebSocket callbacks
  synchronously on drop.

## Repro

Two `createJazzClient` (memory driver, with `serverUrl`) in one route; reload
~30×. A release build traps with "memory access out of bounds" roughly once per
reload.

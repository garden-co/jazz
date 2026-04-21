# Delete stale `SessionClient` HTTP write path (`/sync/object`)

## What

`packages/jazz-tools/src/runtime/client.ts:554-641` defines
`SessionClient.create`, `SessionClient.update`, and `SessionClient.delete`
methods that `fetch()` against `/sync/object` and `/sync/object/delete`. The
Rust server registers no matching route (`crates/jazz-tools/src/routes.rs`
only exposes `/schema/*`, `/admin/*`, `/health`, `/ws`). Any caller that lands
here today gets a 404 wrapped in `new Error("Create failed: <statusText>")`.

The real durable-write path on modern clients is `Runtime.insert(...).wait({ tier })`
(WASM/NAPI/UniFFI), which drives writes over the WS transport. So these three
HTTP methods and the `JazzClient.sendRequest` plumbing that supports them are
dead code that still ships bundle weight and a misleading API.

## Notes

- Also strip `JazzClient.sendRequest` and the `/sync/object` URL building if
  they have no other callers after the three methods are removed.
- Keep `SessionClient.query` / `SessionClient.subscribe` — those delegate to
  `client.queryInternal` and are the intended backend surface.
- Surfaces to double-check before ripping:
  - `packages/jazz-tools/src/runtime/client.ts` — remove the three write methods
  - any example in `examples/` that imports `sendRequest` or hits `/sync/object`
  - `packages/jazz-tools/bin/docs-index.txt`
- Land as a separate PR from the React auth refactor.

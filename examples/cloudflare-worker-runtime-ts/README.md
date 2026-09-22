# Cloudflare Worker Runtime Example

Minimal Wrangler example that proves Jazz can boot inside Cloudflare Workers by passing a precompiled Wasm module through `runtimeSources.wasmModule`.

## What it verifies

- `wrangler dev` can bundle the Worker locally
- Cloudflare's Worker runtime can import `jazz-wasm/pkg/jazz_wasm_bg.wasm` as a `WebAssembly.Module`
- `createDb({ account, runtimeSources: { wasmModule } })` can initialize Jazz without relying on browser asset URLs
- The example uses the public `jazz-tools` package and a prepared opaque account handle.
- Each `/smoke` request owns a disposable local-first account and in-memory database, then shuts it down. Requests never share a user context. This demonstrates runtime initialization, not durable storage or authentication.

For an authenticated application, use your core registry URL and `accounts.loginJWT(...)` before opening the request context. Use an appropriate durable account store when retaining local-first signing roots. The smoke example supplies an ephemeral request-local store and omits the context server URL, so it performs no sync or registry request.

## Run locally

```bash
pnpm --filter cloudflare-worker-runtime-ts dev
```

Then in another terminal:

```bash
curl http://127.0.0.1:8787/smoke
```

## Remote preview

```bash
pnpm --filter cloudflare-worker-runtime-ts dev:remote
```

Use `dev:remote` only when you specifically want Cloudflare-network execution. Local `wrangler dev` runs the Worker on local `workerd` via Miniflare, so it is the fast same-runtime-engine smoke-test path; `dev:remote` is still closer to deployed Cloudflare bindings and edge behavior.

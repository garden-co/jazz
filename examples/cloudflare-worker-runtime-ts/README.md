# Cloudflare Worker Runtime Example

Minimal Wrangler example that proves Jazz can boot inside Cloudflare Workers by passing a precompiled Wasm module through `runtimeSources.wasmModule`.

## What it verifies

- `wrangler dev` can bundle the Worker locally
- Cloudflare's Worker runtime can import `jazz-wasm/pkg/jazz_wasm_bg.wasm` as a `WebAssembly.Module`
- `createJazzClient({ serverUrl: null, initial: "device", runtimeSources: { wasmModule }, … })` can initialize Jazz without relying on browser asset URLs; `session.db` is its database
- The example uses the public `jazz-tools` package.
- Each `/smoke` request owns a disposable device account and in-memory database, then closes its session. Requests never share a user context. This demonstrates runtime initialization, not durable storage or authentication.

The smoke example supplies an ephemeral request-local store and sets `serverUrl: null`, so it performs no sync or registry request.

`createJazz` (`jazz-tools/backend`) does not run in a Worker: it loads Jazz's native Node addon (`jazz-napi`), which workerd cannot load. For an authenticated application, run your backend's `createJazz` on Node (your own server or Vercel); its data can live in a Cloudflare Durable Object with the `durableObjects()` store (see the Cloudflare Durable Objects deployment docs).

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

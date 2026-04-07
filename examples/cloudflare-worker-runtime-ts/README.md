# Cloudflare Worker Runtime Example

Minimal Wrangler example that proves Jazz can boot inside Cloudflare Workers by passing a precompiled Wasm module through `runtimeSources.wasmModule`.

## What it verifies

- `wrangler dev` can bundle the Worker locally
- Cloudflare's Worker runtime can import `jazz-wasm/pkg/jazz_wasm_bg.wasm` as a `WebAssembly.Module`
- `createDb({ runtimeSources: { wasmModule } })` can initialize Jazz without relying on browser asset URLs
- The repo-local example imports Jazz from source so it is not blocked on packaging `jazz-tools` first

## Run locally

```bash
pnpm --filter cloudflare-worker-runtime-ts dev
```

Then in another terminal:

```bash
curl http://127.0.0.1:8787/smoke
curl http://127.0.0.1:8787/todos
```

## Remote preview

```bash
pnpm --filter cloudflare-worker-runtime-ts dev:remote
```

Use `dev:remote` only when you specifically want Cloudflare-network execution. Local `wrangler dev` runs the Worker on local `workerd` via Miniflare, so it is the fast same-runtime-engine smoke-test path; `dev:remote` is still closer to deployed Cloudflare bindings and edge behavior.

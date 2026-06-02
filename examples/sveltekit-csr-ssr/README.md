# Jazz + SvelteKit CSR / SSR example

The SvelteKit counterpart to `examples/nextjs-csr-ssr`: three columns reading and writing the same Jazz database, side by side.

- **Client-side (Svelte)** — a browser Jazz client reads and writes the shared list live.
- **Server-side (load)** — a `+page.server.ts` `load` reads the rows via `jazz-tools/backend`; writes go through a form action and the load re-runs. Not a live subscription.
- **Server prefetch + client hydrate** — the `load` prefetches and dehydrates a snapshot; `JazzSvelteProvider` seeds it so the rows are already in the SSR HTML, then a browser client takes over and updates stream in live.

## Run it

```sh
pnpm install
pnpm --filter sveltekit-csr-ssr dev
```

The `jazzSvelteKit()` Vite plugin's managed runtime provisions a local Jazz app, a sync server, and the `PUBLIC_JAZZ_APP_ID` / `PUBLIC_JAZZ_SERVER_URL` / `BACKEND_SECRET` env vars, so no `.env` is needed for local dev.

## Hot points

- `vite.config.ts` uses `jazzSvelteKit()` from `jazz-tools/dev/sveltekit`.
- `src/lib/server/jazz.ts` is server-only (the `$lib/server` directory is never bundled into client code) — it holds `BACKEND_SECRET` and the backend `Db`.
- The snapshot is prefetched with `asBackend()`, so it is public (`null` principal) and seeds into any session — safe here because every todo is public. For per-user pages, scope the prefetch with `context.forRequest(event.request)` and keep the response private; the rows still render in the SSR HTML. See the [server-rendering recipe](https://jazz.tools/docs/recipes/server-rendering/hydrate-prerendered-queries).

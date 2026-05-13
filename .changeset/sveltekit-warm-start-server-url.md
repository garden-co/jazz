---
"jazz-tools": patch
---

`jazzSvelteKit` now restarts the Vite dev server on warm starts too when `PUBLIC_JAZZ_SERVER_URL` was missing from SvelteKit's captured env. Previously the restart only fired on the first-ever cold start (when no `PUBLIC_JAZZ_APP_ID` was persisted yet). On every subsequent run, the local Jazz server was allocated on a fresh dynamic port and written to `process.env`, but SvelteKit's `config({ order: 'pre' })` hook had already captured env, so `$env/dynamic/public.PUBLIC_JAZZ_SERVER_URL` stayed `undefined` and apps could not reach the dev server. The restart is now triggered whenever either `PUBLIC_JAZZ_APP_ID` or `PUBLIC_JAZZ_SERVER_URL` was missing from `process.env` before initialisation; `process.env` survives the restart and `runtime.initialize()` is idempotent, so the post-restart pass reuses the in-flight Jazz server with no reallocation.

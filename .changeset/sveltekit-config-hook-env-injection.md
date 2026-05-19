---
"jazz-tools": patch
---

`jazzSvelteKit` now starts the managed Jazz dev server and populates `PUBLIC_JAZZ_APP_ID` / `PUBLIC_JAZZ_SERVER_URL` from an `enforce: "pre"` Vite `config` hook, before SvelteKit's `vite-plugin-sveltekit-setup` captures env into `$env/dynamic/public`. The previous approach set these in `configureServer` — after SvelteKit had already frozen its env — and recovered by triggering a fire-and-forget dev-server restart. On a freshly scaffolded starter the first paint reliably rendered with `PUBLIC_JAZZ_SERVER_URL` undefined, and recovery was race-dependent. The restart is removed entirely; the dynamically allocated server URL is now correct on the first request for both cold and warm starts, matching how the Next.js plugin awaits `runtime.initialize()` during config resolution. Plugin order in `vite.config.ts` is no longer load-bearing.

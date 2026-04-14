---
"jazz-tools": patch
---

Added `jazzSvelteKit()` Vite plugin (`jazz-tools/dev/sveltekit`) for SvelteKit and Vite+Svelte projects. Starts an embedded Jazz dev server, publishes and watches the schema, and injects `PUBLIC_JAZZ_APP_ID`/`PUBLIC_JAZZ_SERVER_URL` into the Vite env. Supports three modes: embedded local server (default), connect to an explicit URL via `server: "https://…"`, or connect to a server already described in `PUBLIC_JAZZ_SERVER_URL`. Defaults `schemaDir` to `src/lib/` to match SvelteKit conventions.

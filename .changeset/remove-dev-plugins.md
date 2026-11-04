---
"jazz-tools": patch
---

Breaking: remove the dev plugins. `jazz-tools/dev/next` (`withJazz`), `jazz-tools/dev/vite` (`jazzPlugin`), `jazz-tools/dev/sveltekit` and `jazz-tools/dev/expo` are gone, along with the managed dev server, schema watcher and env-file loading they ran. Jazz runs inside your own backend through `createJazz`, so there is no separate dev server to start. What's left is plain bundler config:

- Next.js: `serverExternalPackages: ["jazz-napi"]`.
- Vite, for the local-first client: `optimizeDeps: { exclude: ["jazz-wasm"] }`, with `jazz-wasm` as a direct dependency of your app (the plugin used to alias it for pnpm's strict layout).

`jazz-tools/dev` and `jazz-tools/testing` keep `startLocalJazzServer` and `deploy()` for tests and fixtures.

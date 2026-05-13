# todo-client-localfirst-ts

Local-first todo app in vanilla TypeScript + Vite — no UI framework. Anonymous auth via `BrowserAuthSecretStore`, OPFS persistence.

## What it demonstrates

- Anonymous identity from a locally generated secret — no login UI, no auth server.
- The low-level Jazz client API: `createDb`, `db.subscribeAll`, `db.insert` / `db.update` / `db.delete`, `db.onAuthChanged` — consumed without React, Svelte, or Vue bindings.
- Row-level permissions — `owner_id` enforced by `definePermissions`; mutations on rows you don't own are rejected by the runtime.
- OPFS-backed persistence across reload, plus optional server sync when `VITE_JAZZ_SERVER_URL` is set.
- The Jazz Vite plugin (`jazzPlugin` in `vite.config.ts`) hosts the dev sync server alongside Vite.

## Schema

- **projects** — name
- **todos** — title, done, description, owner_id, parentId (self-ref, optional), projectId (optional)

## Running locally

```bash
pnpm dev
```

`pnpm dev` starts the Jazz dev server and the Vite dev server together via the Jazz Vite plugin. The plugin writes `VITE_JAZZ_APP_ID` / `VITE_JAZZ_SERVER_URL` into the dev environment automatically.

## Tests

```bash
pnpm test
```

Vitest browser-mode integration tests (chromium) covering OPFS persistence across remount and core ↔ edge sync between two app instances.

# todo-client-localfirst-ts

Local-first todo app in vanilla TypeScript + Vite — no UI framework. Anonymous auth via `BrowserAuthSecretStore`, IndexedDB persistence.

## What it demonstrates

- Anonymous identity from a locally generated secret — no login UI, no auth server.
- The low-level Jazz client API: `createDb`, `db.subscribe`, `db.insert` / `db.update` / `db.delete`, `db.onAuthChanged` — consumed without React, Svelte, or Vue bindings.
- Row-level permissions — `owner_id` enforced by `definePermissions`; mutations on rows you don't own are rejected by the runtime.
- IndexedDB-backed persistence across reload, synced through the app's own backend.
- Jazz inside the app's backend: `jazz.server.ts` runs `createJazz` in the Vite server, so pages sync on their own origin under `/jazz`. In production, run the same `createJazz` in your server.

## Schema

- **projects** — name
- **todos** — title, done, description, owner_id, parentId (self-ref, optional), projectId (optional)

## Running locally

```bash
pnpm dev
```

`pnpm dev` starts Vite with Jazz inside it (`jazz.server.ts`); data stays in `.jazz/`.

## Tests

```bash
pnpm test
```

Vitest browser-mode integration tests (chromium) covering IndexedDB persistence across remount and sync between two app instances through the backend.

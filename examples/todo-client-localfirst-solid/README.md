# todo-client-localfirst-solid

Local-first todo app on Solid + Vite, anonymous auth via `useLocalFirstAuth`, IndexedDB persistence.

## What it demonstrates

- Anonymous identity from a locally generated secret — no login UI, no auth server.
- `useAll` reactive queries with composable `where()` filters.
- Synchronous local writes through the `db` handle on the `useAll` result (`todos.db.insert`, `todos.db.update`, `todos.db.delete`).
- Row-level permissions — `owner_id` enforced by `definePermissions`; invalid mutations surface as toast errors via `solid-sonner`.
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

Vitest browser-mode integration tests (chromium) covering CRUD, IndexedDB persistence across remount, and server sync between two app instances.

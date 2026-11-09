# todo-client-localfirst-react

Local-first todo app on React + Vite, anonymous auth via `useLocalFirstAuth`, IndexedDB persistence.

## What it demonstrates

- Anonymous identity from a locally generated secret — no login UI, no auth server.
- `useAll` reactive queries with composable `where()` filters (filter by title substring, by done status).
- Synchronous local writes through the `db` handle returned by `useAll` (`db.insert`, `db.update`, `db.delete`).
- Row-level permissions — `owner_id` enforced by `definePermissions`; mutations on rows you don't own surface as toast errors via `sonner`.
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

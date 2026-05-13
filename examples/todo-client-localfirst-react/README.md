# todo-client-localfirst-react

Local-first todo app on React + Vite, anonymous auth via `useLocalFirstAuth`, OPFS persistence.

## What it demonstrates

- Anonymous identity from a locally generated secret — no login UI, no auth server.
- `useAll` reactive queries with composable `where()` filters (filter by title substring, by done status).
- `useDb` synchronous local writes (`db.insert`, `db.update`, `db.delete`).
- Row-level permissions — `owner_id` enforced by `definePermissions`; mutations on rows you don't own surface as toast errors via `sonner`.
- OPFS-backed persistence across reload, plus optional server sync when `VITE_JAZZ_SERVER_URL` is set.
- `JazzProvider` wired up via the Jazz Vite plugin (`jazzPlugin` in `vite.config.ts`).
- DevTools integration via `attachDevTools`.

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

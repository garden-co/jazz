# todo-client-localfirst-solid

Local-first todo app on Solid + Vite, anonymous auth via `useLocalFirstAuth`, IndexedDB persistence.

## What it demonstrates

- Anonymous identity from a locally generated secret — no login UI, no auth server.
- `useAll` reactive queries with composable `where()` filters.
- `useDb` synchronous local writes (`db.insert`, `db.update`, `db.delete`).
- Row-level permissions — `owner_id` enforced by `definePermissions`; invalid mutations surface as toast errors via `solid-sonner`.
- IndexedDB-backed persistence across reload, plus optional server sync when `VITE_JAZZ_SERVER_URL` is set.
- `pnpm dev` starts a local Jazz core next to Vite (`dev/examples/with-local-core.mjs`) and passes its URL in `VITE_JAZZ_SERVER_URL`.

## Schema

- **projects** — name
- **todos** — title, done, description, owner_id, parentId (self-ref, optional), projectId (optional)

## Running locally

```bash
pnpm dev
```

`pnpm dev` starts a local Jazz core and the Vite dev server together (`dev/examples/with-local-core.mjs`), which passes `VITE_JAZZ_APP_ID` / `VITE_JAZZ_SERVER_URL` to the dev environment.

## Tests

```bash
pnpm test
```

Vitest browser-mode integration tests (chromium) covering CRUD, IndexedDB persistence across remount, and server sync between two app instances.

# todo-client-localfirst-solid

Local-first todo app on Solid + Vite, anonymous auth via `useLocalFirstAuth`, OPFS persistence.

## What it demonstrates

- Anonymous identity from a locally generated secret — no login UI, no auth server.
- `useAll` reactive queries with composable `where()` filters.
- `useDb` synchronous local writes (`db.insert`, `db.update`, `db.delete`).
- Row-level permissions — `owner_id` enforced by `definePermissions`; invalid mutations surface as toast errors via `solid-sonner`.
- OPFS-backed persistence across reload, plus optional server sync when `VITE_JAZZ_SERVER_URL` is set.
- `JazzProvider` + `createSolidJazzClient` wired up via the Jazz Vite plugin (`jazzPlugin` in `vite.config.ts`).

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

Vitest browser-mode integration tests (chromium) covering CRUD, OPFS persistence across remount, and server sync between two app instances.

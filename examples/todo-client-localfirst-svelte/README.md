# todo-client-localfirst-svelte

Local-first todo app on Svelte 5 + Vite, anonymous auth via local-first secret, OPFS persistence.

## What it demonstrates

- Anonymous identity from a locally generated secret — no login UI, no auth server.
- `QuerySubscription` reactive live queries with composable `where()` filters (filter by title substring, by done status).
- `getDb` synchronous local writes (`db.insert`, `db.update`, `db.delete`).
- Row-level permissions — `owner_id` enforced by `definePermissions`; mutations on rows you don't own surface as toast errors via `svelte-sonner`.
- OPFS-backed persistence across reload, plus optional server sync when `PUBLIC_JAZZ_SERVER_URL` is set.
- `JazzSvelteProvider` wired up via the Jazz Vite plugin (`jazzPlugin` in `vite.config.ts`).

## Schema

- **projects** — name
- **todos** — title, done, description, owner_id, parentId (self-ref, optional), projectId (optional)

## Running locally

```bash
pnpm dev
```

`pnpm dev` starts the Jazz dev server and the Vite dev server together via the Jazz Vite plugin. The plugin writes `PUBLIC_JAZZ_APP_ID` / `PUBLIC_JAZZ_SERVER_URL` into the dev environment automatically.

## Tests

```bash
pnpm test
```

Vitest browser-mode integration tests (chromium) covering OPFS persistence across remount and core ↔ edge sync between two app instances.

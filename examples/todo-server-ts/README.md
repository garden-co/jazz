# todo-server-ts

Node + Express REST API backed by Jazz as the database. No frontend — pure server-side TypeScript, persistent Fjall storage via the Jazz NAPI bindings.

## What it demonstrates

- Using Jazz as a server-side backend via `jazz-tools/backend` and `createJazzContext` — no browser, no WASM.
- CRUD over `/todos` (`GET`, `POST`, `PUT /:id`, `DELETE /:id`) with row-level permissions enforced server-side.
- Per-session policy evaluation via `context.forSession(userId)` — the `/todos/as/:userId` endpoint impersonates a session so `definePermissions` filters rows by `owner_id`.
- Server-Sent Events (`/todos/live`) pushing live snapshots to connected clients on every mutation.
- Write durability control via `wait({ tier })` (`local`, `edge`, `global`).
- Persistent Fjall storage rooted in a temp directory on cold start.

## Schema

- **projects** — name
- **todos** — title, done, description, owner_id, parentId (self-ref, optional), projectId (optional)

## Running locally

```bash
pnpm dev
```

`pnpm dev` runs the server with `tsx watch` against `src/main.ts`. The HTTP API listens on a default port (see `main.ts`); a fresh Fjall database is created in a temp directory.

## Tests

```bash
pnpm test
```

Vitest integration tests cover the CRUD lifecycle, session-scoped reads, and persistence/cold-start.

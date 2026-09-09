# todo-server-ts

Node + Express REST API backed by Jazz as the database. No frontend — pure server-side TypeScript, with persistent Fjall storage via the Jazz NAPI bindings.

## What it demonstrates

- Using Jazz as a server-side backend via `jazz-tools/backend` and `createJazzSession` — no browser, no WASM.
- Authenticated CRUD over `/todos` (`GET`, `POST`, `PUT /:id`, `DELETE /:id`) with row-level permissions enforced server-side.
- Request authentication through `client.forRequest(req)`. Every `/todos` request sends `Authorization: Bearer <token>`; Jazz verifies the token and derives the session owner.
- Server-Sent Events (`/todos/live`) pushing only the authenticated caller's live snapshot on every mutation.
- Write durability control via `wait({ tier })` (`local`, `edge`, `global`).
- Explicit persistent or in-memory storage selection for programmatic servers and the CLI.

## Schema

- **projects** — name
- **todos** — title, done, description, owner_id, parentId (self-ref, optional), projectId (optional)

## Running locally

```bash
pnpm dev
```

`pnpm dev` runs the server with `tsx watch` against `src/main.ts`. The HTTP API listens on a default port (see `main.ts`).

Storage is persistent by default. The CLI resolves its database path in this order:

1. `--data-path <path>` (if supplied).
2. A non-empty `DB_PATH` environment variable.
3. `./data/todos/<effective-app-id-base64url>/jazz.db`, relative to the current working directory. The effective app ID is `JAZZ_APP_ID`, or the example's default when it is unset.

`--in-memory` is the only volatile mode. It conflicts with `--data-path` and with any set `DB_PATH`, including an empty value. Unknown options, missing or empty `--data-path` values, and an explicitly empty `DB_PATH` fail before the server listens. Explicit paths are operator-owned: storage-open or lock errors are not redirected to another path.

## Authentication

The API accepts Jazz local-first identity proofs as bearer tokens. A local-first client can mint
one with `db.getLocalFirstIdentityProof()` and send it on every todo request:

```bash
curl -H "Authorization: Bearer $JAZZ_TOKEN" http://localhost:3000/todos
```

For an external auth provider, set either `JAZZ_JWKS_URL` or `JAZZ_JWT_PUBLIC_KEY` before
starting the server. The JWT's verified `sub` claim becomes the Jazz session's `user_id`.
Clients never send `owner_id`; `POST /todos` always assigns the authenticated session owner.
The `/health` endpoint remains public.

## Tests

```bash
pnpm test
```

Vitest integration tests cover request authentication, owner-scoped CRUD and live updates, persistence/cold-start, and a real CLI process restart.

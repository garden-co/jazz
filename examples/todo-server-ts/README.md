# todo-server-ts

Node + Express REST API backed by Jazz as the database. No frontend: pure server-side TypeScript, with Jazz running inside the server (`createJazz`) on SQLite, or on a Jazz core elsewhere.

## What it demonstrates

- Running Jazz inside a Node backend with `createJazz` from `jazz-tools/backend`: no browser, no WASM.
- Authenticated CRUD over `/todos` (`GET`, `POST`, `PUT /:id`, `DELETE /:id`) with row-level permissions enforced server-side.
- Request authentication in the app's own `authenticate`: every `/todos` request sends `Authorization: Bearer <jwt>`, `fromJWT` verifies it against the identity provider's JWKS, and `jazz.forReqUser(request)` opens the database as that user.
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

`JAZZ_JWKS_URL` and `JAZZ_JWT_ISSUER` are required: the JWKS endpoint and issuer of the identity provider whose JWTs the API accepts. Set `JAZZ_JWT_AUDIENCE` too when the issuer signs tokens for other apps.

Jazz runs in the server process and keeps its data in SQLite. To keep the data on a Jazz core elsewhere (Jazz Cloud, or a sync server you run) instead, set `JAZZ_SERVER_URL`, `JAZZ_APP_ID` and `JAZZ_ADMIN_SECRET`. `createJazz` publishes the app's schema and permissions on start, so there is no separate deploy step.

Storage is persistent by default. The CLI resolves its database path in this order:

1. `--data-path <path>` (if supplied).
2. A non-empty `DB_PATH` environment variable.
3. `./data/todos/<effective-app-id-base64url>/jazz.db`, relative to the current working directory. The effective app ID is `JAZZ_APP_ID`, or the example's default when it is unset.

`--in-memory` is the only volatile mode. It conflicts with `--data-path` and with any set `DB_PATH`, including an empty value. Unknown options, missing or empty `--data-path` values, and an explicitly empty `DB_PATH` fail before the server listens. Explicit paths are operator-owned: storage-open or lock errors are not redirected to another path.

## Authentication

Who a request is, this server decides: its `authenticate` (`fromJWT`) verifies the bearer JWT
against `JAZZ_JWKS_URL` and `JAZZ_JWT_ISSUER` and hands Jazz the token's `sub`. Jazz gives each user their own account the
first time it sees them; `POST /sign-in` returns it.

```bash
curl -H "Authorization: Bearer $JWT" http://localhost:3000/todos
```

Clients never send `owner_id`; `POST /todos` always assigns the authenticated session owner.
The `/health` endpoint remains public.

## Tests

```bash
pnpm test
```

Vitest integration tests cover request authentication, owner-scoped CRUD and live updates, persistence/cold-start, and a real CLI process restart.

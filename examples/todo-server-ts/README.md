# todo-server-ts

Node + Express REST API backed by Jazz as the database. No frontend — pure server-side TypeScript, persistent Fjall storage via the Jazz NAPI bindings.

## What it demonstrates

- Using Jazz as a server-side backend via `jazz-tools/backend` and `createJazzContext` — no browser, no WASM.
- Authenticated CRUD over `/todos` (`GET`, `POST`, `PUT /:id`, `DELETE /:id`) with row-level permissions enforced server-side.
- Request authentication through `context.forRequest(req)`. Every `/todos` request sends `Authorization: Bearer <token>`; Jazz verifies the token and derives the session owner.
- Server-Sent Events (`/todos/live`) pushing only the authenticated caller's live snapshot on every mutation.
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

Vitest integration tests cover request authentication, owner-scoped CRUD and live updates, and persistence/cold-start.

# react-selfhosted-hybrid

A Vite + React starter for [Jazz](https://jazz.tools) with hybrid auth: the app
works immediately as a local-first experience, and users can optionally sign up
with [Better Auth](https://better-auth.com) to bind their local identity to an
account. A lightweight [Hono](https://hono.dev) server handles auth.

## What this starter gives you

- A todo list that works immediately — no sign-in required.
- Optional sign-up that cryptographically binds the local-first identity to a
  Better Auth account via `generateLocalFirstIdentityProof`.
- Better Auth handling email/password sign-up, sign-in, sign-out, and JWTs.
- A tiny Hono backend serving only `/api/auth/*` — no other API surface.
- A local Jazz dev server started automatically by the `jazzPlugin` Vite plugin
  in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.

> [!TIP]
> If you want auth required upfront (no local-first onboarding), use `react-selfhosted-betterauth` instead. If you want no auth at all, use `react-selfhosted-localfirst`.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173). The app loads immediately
with a local identity — data is persisted locally. Sign up to bind that identity
to an email/password account so it survives across devices or browsers.
Set `BETTER_AUTH_SECRET` in `.env` before running (`openssl rand -base64 32`
or scaffold via `create-jazz`).

## Architecture

```
src/
  main.tsx                       ← app entry; mounts BetterAuthProvider + JazzProvider
  App.tsx                        ← conditional render: sign-up/in forms or todo list
  sign-in-form.tsx               ← sign-in form
  sign-up-form.tsx               ← sign-up form (generates local-first identity proof)
  todo-widget.tsx                ← Jazz-powered todo list
  auth-client.ts                 ← Better Auth React client
  App.css
server/
  app.ts                         ← Hono app; mounts BetterAuth at /api/auth/*
  auth.ts                        ← Better Auth server config (hybrid proof verification)
  index.ts                       ← @hono/node-server entry; listens on port 3001
schema.ts                        ← Jazz app schema (todos table)
permissions.ts                   ← row-level access policy ($createdBy)
scripts/
  init-secret.mjs                ← generates BETTER_AUTH_SECRET in .env
```

## How it works

Two processes run in development:

1. **Hono** on port 3001, serving `/api/auth/*` (sign-up, sign-in, token, JWKS).
2. **Vite** on port 5173, proxying `/api/*` to Hono.

On sign-up, `src/sign-up-form.tsx` calls `db.getLocalFirstIdentityProof()` to
generate a short-lived cryptographic proof that binds the local Jazz identity to
the sign-up request. This proof is sent alongside the email/password to the Hono
server, where `server/auth.ts` verifies it via `verifyLocalFirstIdentityProof`
from `jazz-napi`. On success, BetterAuth creates the user with the proved
identity ID, so the Jazz principal carries over seamlessly.

Both sides use the audience string `"react-localfirst-signup"` — the
client proof and the server verification must match.

`BetterAuthProvider` in `src/main.tsx` watches the Better Auth session. When a
session exists, it fetches a JWT and passes it to `<JazzProvider>` as
`jwtToken`. The Jazz dev server verifies that JWT against the JWKS endpoint.

## Extending the schema

Edit `schema.ts` to add tables. The Jazz dev server watches the file and
republishes the schema on change — no restart needed.

```ts
const schema = {
  todos: s.table({ title: s.string(), done: s.boolean() }),
  projects: s.table({ name: s.string() }),
};
```

Row ownership is enforced by `permissions.ts` via the `$createdBy` predicate,
so you don't need an explicit `ownerId` column. Jazz records the creating
session on every row and the permission policy scopes reads/writes to it.

## Environment variables

Scaffold via `create-jazz` to have `.env` populated automatically; otherwise
write the values below by hand.

| Variable               | When       | Purpose                                                           |
| ---------------------- | ---------- | ----------------------------------------------------------------- |
| `BETTER_AUTH_SECRET`   | always     | BetterAuth session signing. `server/auth.ts` throws if missing.   |
| `PORT`                 | optional   | Hono server port (default `3001`).                                |
| `VITE_JAZZ_APP_ID`     | cloud only | Provisioned app ID. Unset in self-hosted dev — plugin injects it. |
| `VITE_JAZZ_SERVER_URL` | cloud only | Cloud sync URL (e.g. `https://prod.v2.aws.cloud.jazz.tools`).     |
| `JAZZ_ADMIN_SECRET`    | cloud only | Admin credential for schema pushes to the cloud.                  |
| `BACKEND_SECRET`       | cloud only | Backend signing credential.                                       |

Generate a dev `BETTER_AUTH_SECRET` with `openssl rand -base64 32`. In
self-hosted mode (no cloud env vars), the `jazzPlugin` plugin spawns a local
Jazz dev server and supplies its own credentials.

## Deploying to production

Build and start with:

```bash
pnpm build   # vite build + tsc -p tsconfig.server.json
pnpm start   # node server-dist/index.js (serves SPA + /api/auth/*)
```

Hono serves the built SPA from `./dist` alongside the auth routes. Supply
`BETTER_AUTH_SECRET` through your hosting provider's secret management — the
value must be consistent across restarts, as rotating it invalidates all
existing sessions.

Better Auth's in-memory adapter (`server/auth.ts`) is a placeholder. Swap it
for a persistent database adapter before shipping, or users will be wiped on
every process restart.

## Known limitations

- **In-memory user store.** The Better Auth memory adapter keeps everything
  per-process, so restarts reset all accounts. Swap for a persistent adapter
  before shipping.

## Where to go next

- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.

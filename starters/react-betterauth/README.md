# react-selfhosted-betterauth

A Vite + React starter for [Jazz](https://jazz.tools) with a
[Better Auth](https://better-auth.com) sign-in gate, backed by a lightweight
[Hono](https://hono.dev) server. The app is only accessible to authenticated users.

## What this starter gives you

- A sign-in / sign-up form, and a todo list which uses Jazz for persistence,
  accessible to signed-in users.
- Better Auth handling email/password sign-up, sign-in, sign-out, and JWTs.
- A tiny Hono backend serving only `/api/auth/*` — no other API surface.
- A local Jazz dev server started automatically by the `jazzPlugin` Vite plugin
  in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.

> [!TIP]
> If you want local-first onboarding with an optional upgrade, use `react-selfhosted-hybrid` instead. If you want no auth at all, use `react-selfhosted-localfirst`.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173), create an account, and
you'll land on the todo list persisted via Jazz. `pnpm install` seeds
`.env` with a random `BETTER_AUTH_SECRET` and `BACKEND_SECRET`. The Vite
dev command launches Hono on port 3001 and waits for its `/health`
endpoint before starting Vite.

## Architecture

```
src/
  main.tsx                       ← app entry; mounts BetterAuthProvider + JazzProvider
  App.tsx                        ← conditional render: sign-in form or todo list
  sign-in-form.tsx               ← sign-in / sign-up form (toggle between modes)
  todo-widget.tsx                ← Jazz-powered todo list
  auth-client.ts                 ← Better Auth React client
  App.css
server/
  app.ts                         ← Hono app; mounts BetterAuth at /api/auth/*
  auth.ts                        ← Better Auth server config (JWT plugin, memory DB)
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

`BetterAuthProvider` in `src/main.tsx` watches the Better Auth session. When a
session exists, it fetches a JWT via `authClient.token()` and passes it to
`<JazzProvider>` as `jwtToken`. The Jazz dev server verifies that JWT against
the JWKS endpoint at `http://localhost:3001/api/auth/jwks`, whose URL is
declared in `vite.config.ts` so the plugin can wire it up automatically.

A `JwtRefresh` component inside the provider re-mints the JWT whenever
`db.onAuthChanged` reports the token as expired, so long-lived sessions stay
authenticated silently.

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

## Local development

Two processes run in parallel (`concurrently`): Hono on port 3001 for the
BetterAuth HTTP handlers, and Vite on port 5173 for the React bundle. Vite's
`dev:vite` script gates on `wait-on http-get://127.0.0.1:3001/health`, so
the browser bundle only serves once Hono is ready to accept requests.

The `jazzPlugin` Vite plugin spawns a local Jazz dev server on a
random port and writes `VITE_JAZZ_APP_ID` / `VITE_JAZZ_SERVER_URL` to
`.env` on first `pnpm dev`. Subsequent runs pick them up from `.env`.

## Environment variables

`pnpm install` runs `scripts/ensure-env.js`, which seeds any missing keys
in `.env` with random values. Override by setting values manually before
install, or for cloud mode scaffold via `create-jazz --hosting hosted`.

| Variable               | When       | Purpose                                                                       |
| ---------------------- | ---------- | ----------------------------------------------------------------------------- |
| `BETTER_AUTH_SECRET`   | always     | BetterAuth session signing. `server/auth.ts` throws if missing.               |
| `PORT`                 | optional   | Hono server port (default `3001`).                                            |
| `VITE_JAZZ_APP_ID`     | cloud only | Provisioned app ID. Unset in self-hosted dev — plugin injects it.             |
| `VITE_JAZZ_SERVER_URL` | cloud only | Cloud sync URL (e.g. `https://v2.sync.jazz.tools`).                           |
| `JAZZ_ADMIN_SECRET`    | cloud only | Admin credential for schema pushes to the cloud.                              |
| `BACKEND_SECRET`       | always     | Persistent identity for the backend's Jazz account. Seeded by the scaffolder. |

In self-hosted mode (no cloud env vars), the `jazzPlugin` plugin spawns a
local Jazz dev server and supplies its own credentials.

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

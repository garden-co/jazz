# ts-betterauth

A Vite + plain-TypeScript starter for [Jazz](https://jazz.tools) with
[Better Auth](https://better-auth.com) email/password sign-up gating all
access. A lightweight [Hono](https://hono.dev) server handles auth. No UI
framework — just `document.createElement` and direct DOM updates inside the
Jazz subscription callback.

## What this starter gives you

- Email/password sign-up and sign-in required upfront — no anonymous access.
- Better Auth handling sign-up, sign-in, sign-out, and short-lived JWTs.
- A tiny Hono backend serving only `/api/auth/*` — no other API surface.
- A local Jazz dev server started automatically by the `jazzPlugin` Vite plugin
  in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.

> [!TIP]
> If you want users to be able to try the app before signing up, use `ts-hybrid` instead. If you want no auth at all, use `ts-localfirst`.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173). The app shows a sign-up
form on first load; once you create an account it switches to the todo list.
Set `BETTER_AUTH_SECRET` in `.env` before running (`openssl rand -base64 32`
or scaffold via `create-jazz`).

## Architecture

```
src/
  main.ts                        ← app entry; boots Jazz after BetterAuth session resolves
  app.ts                         ← shell renderer: sign-in form vs todo dashboard
  sign-in-form.ts                ← combined sign-in/sign-up form (mode toggle)
  todo-widget.ts                 ← Jazz-powered todo list (direct DOM)
  auth-client.ts                 ← Better Auth vanilla client
  app.css
server/
  app.ts                         ← Hono app; mounts BetterAuth at /api/auth/*
  auth.ts                        ← Better Auth server config
  index.ts                       ← @hono/node-server entry; listens on port 3001
schema.ts                        ← Jazz app schema (todos table)
permissions.ts                   ← row-level access policy ($createdBy)
scripts/
  ensure-env.js                  ← generates BETTER_AUTH_SECRET in .env
```

## How it works

Two processes run in development:

1. **Hono** on port 3001, serving `/api/auth/*` (sign-up, sign-in, token, JWKS).
2. **Vite** on port 5173, proxying `/api/*` to Hono.

`src/accounts.ts` prepares an `AccountManager`, including its encrypted local
storage. `src/jazz-lifecycle.ts` owns exactly one `Db`, opened with the
manager's opaque account handle. It obtains a fresh Better Auth JWT through
`authClient.$fetch("/token")` whenever Jazz needs one; the JWT is never stored
as the account identity.

The initial session and ordinary sign-ins call `loginJWT`; only the explicit
sign-up/recovery action calls `registerJWT`. Session changes are serialized
with form actions. Before a sign-out or account change, the old context shuts
down with `waitForSync: true`; a failed close leaves it usable, and a failed
account operation reopens the manager's selected handle so the recovery button
can be retried safely.

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

`pnpm dev` runs the Hono server and the Vite dev server concurrently. The Vite
config proxies `/api/*` to the Hono server. If you change `server/`, `tsx
watch` restarts the Hono process. If you change `src/`, Vite hot-reloads.

## Environment variables

| Variable               | When       | Source                                                |
| ---------------------- | ---------- | ----------------------------------------------------- |
| `BETTER_AUTH_SECRET`   | always     | `scripts/ensure-env.js` (generates on first run)      |
| `APP_ORIGIN`           | always     | `scripts/ensure-env.js` (`http://localhost:3001`)     |
| `VITE_JAZZ_APP_ID`     | cloud only | scaffolder (`create-jazz --hosting hosted`) or manual |
| `VITE_JAZZ_SERVER_URL` | cloud only | scaffolder or manual                                  |
| `JAZZ_ADMIN_SECRET`    | cloud only | scaffolder or manual                                  |
| `BACKEND_SECRET`       | cloud only | scaffolder or manual                                  |

## Deploying to production

For cloud-hosted deployments, set `BETTER_AUTH_SECRET`, `APP_ORIGIN`, and the
four `VITE_JAZZ_*` / `JAZZ_*` / `BACKEND_SECRET` values in your hosting
provider. The Hono server and Vite build are independent and can be deployed
side-by-side or on different hosts as long as the `/api/*` proxy is preserved.

For self-hosted deployments you need to run your own Jazz server pointed at the
Hono server's JWKS endpoint: `jazz-tools server <APP_ID> --jwks-url
https://<your-host>/api/auth/jwks`.

## Known limitations

- **In-memory user store.** `server/auth.ts` uses an in-memory BetterAuth
  adapter. Restart wipes accounts. Swap for a persistent adapter (Prisma,
  Drizzle, etc.) before shipping.
- **One Better Auth secret per environment.** Rotating the secret invalidates
  every existing JWT.

## Where to go next

- `server/auth.ts` — the place to wire up a persistent BetterAuth adapter.
- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.
- `src/main.ts` — the JWT-only auth wiring, expressed as a plain subscription
  callback.

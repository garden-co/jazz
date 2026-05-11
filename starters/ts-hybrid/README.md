# ts-hybrid

A Vite + plain-TypeScript starter for [Jazz](https://jazz.tools) with hybrid
auth: the app works immediately as a local-first experience, and users can
optionally sign up with [Better Auth](https://better-auth.com) to bind their
local identity to an account. A lightweight [Hono](https://hono.dev) server
handles auth. No UI framework — just `document.createElement` and direct DOM
updates inside the Jazz subscription callback.

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
> If you want auth required upfront (no local-first onboarding), use `ts-betterauth` instead. If you want no auth at all, use `ts-localfirst`.

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
  main.ts                        ← app entry; boots Jazz + BetterAuth session subscription
  app.ts                         ← shell renderer: dashboard vs sign-in/up
  sign-in-form.ts                ← sign-in form (vanilla DOM)
  sign-up-form.ts                ← sign-up form (vanilla DOM)
  todo-widget.ts                 ← Jazz-powered todo list (direct DOM)
  auth-backup.ts                 ← recovery phrase + passkey controls
  auth-client.ts                 ← Better Auth vanilla client
  app.css
server/
  app.ts                         ← Hono app; mounts BetterAuth at /api/auth/*
  auth.ts                        ← Better Auth server config (hybrid proof verification)
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

`src/main.ts` boots Jazz once BetterAuth's `useSession` atom has resolved its
initial value. When the session flips between anonymous and signed-in, the
boot loop rebuilds the `Db` against the new config (local-first secret vs
BetterAuth-issued JWT) and re-mounts the widgets.

On sign-up, `src/sign-up-form.ts` calls `db.getLocalFirstIdentityProof()` to
generate a short-lived cryptographic proof that binds the local Jazz identity to
the sign-up request. This proof is sent alongside the email/password to the Hono
server, where `server/auth.ts` verifies it via `verifyLocalFirstIdentityProof`
from `jazz-napi`. On success, BetterAuth creates the user with the proved
identity ID, so the Jazz principal carries over seamlessly.

Both sides use the audience string `"react-localfirst-signup"` — the
client proof and the server verification must match.

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

| Variable               | When       | Source                                                |
| ---------------------- | ---------- | ----------------------------------------------------- |
| `BETTER_AUTH_SECRET`   | always     | `scripts/ensure-env.js` (generates on first run)      |
| `APP_ORIGIN`           | always     | `scripts/ensure-env.js` (`http://localhost:3001`)     |
| `VITE_JAZZ_APP_ID`     | cloud only | scaffolder (`create-jazz --hosting hosted`) or manual |
| `VITE_JAZZ_SERVER_URL` | cloud only | scaffolder or manual                                  |
| `JAZZ_ADMIN_SECRET`    | cloud only | scaffolder or manual                                  |
| `BACKEND_SECRET`       | cloud only | scaffolder or manual                                  |

The Hono server reads `BETTER_AUTH_SECRET` and `APP_ORIGIN` via `tsx
--env-file=.env`. The Vite client reads the `VITE_JAZZ_*` pair, which the
`jazzPlugin` writes for self-hosted mode and the scaffolder writes for cloud
mode.

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
- `src/main.ts` — the local-first ↔ JWT config switching logic, expressed as a
  plain subscription callback.

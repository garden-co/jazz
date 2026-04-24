# sveltekit-betterauth

A SvelteKit starter for [Jazz](https://jazz.tools) with a strict
[Better Auth](https://better-auth.com) sign-in gate. The app is only
accessible to authenticated users.

## What this starter gives you

- A public sign-in / sign-up page, and a simple todo dashboard which uses Jazz
  for persistence, accessible to signed-in users.
- Better Auth handling email/password sign-up, sign-in, sign-out, and JWTs.
- A local Jazz dev server started automatically by the `jazzSvelteKit`
  Vite plugin in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.

> [!TIP]
> If you want local-first onboarding with an optional upgrade, use `sveltekit-hybrid` instead. If you want only local-first auth, use `sveltekit-localfirst`.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173), create an account,
and you'll land on `/dashboard` with a working todo list persisted via
Jazz. `pnpm install` seeds `.env` with a random `BETTER_AUTH_SECRET` and
`BACKEND_SECRET`; the `jazzSvelteKit` plugin spawns a local Jazz dev
server automatically.

## Architecture

```
src/
  app.html                        ← HTML shell
  app.css                         ← global styles
  app.d.ts                        ← SvelteKit types
  hooks.server.ts                 ← Better Auth handler + cookie route gate
  lib/
    schema.ts                     ← Jazz app schema (todos table)
    permissions.ts                ← row-level access policy ($createdBy)
    TodoWidget.svelte             ← Jazz-powered todo list
    auth.ts                       ← Better Auth server config
    auth-client.ts                ← Better Auth Svelte client
  routes/
    +layout.svelte                ← plain root layout
    +page.svelte                  ← public sign-in / sign-up form (redirects signed-in users to /dashboard)
    (authenticated)/
      +layout.svelte              ← one-shot JWT fetch + JazzSvelteProvider
      dashboard/
        +page.svelte              ← greeting, sign-out, <TodoWidget />
```

## How it works

Route protection is handled by `hooks.server.ts`, which routes all
`/api/auth/*` traffic through `svelteKitHandler` and checks the session
cookie on every other request — redirecting `/` to `/dashboard` for
signed-in users, and `/dashboard/*` back to `/` for signed-out users.
This uses `getSessionCookie`, a cheap cookie-presence check, not a full
DB read.

`src/routes/(authenticated)/+layout.svelte` fetches a Better Auth JWT
once on mount and passes it to `createJazzClient`, which drives a
`<JazzSvelteProvider>`. Because the hook guarantees a session on
`/dashboard/*`, the provider is only mounted when the user is
authenticated — there's no anonymous fallback path to reason about. The
same layout installs a `db.onAuthChanged` listener that re-mints the JWT
whenever Better Auth reports it as expired, so long-lived sessions won't
silently drop to unauthenticated.

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

`pnpm install` runs `scripts/ensure-env.js`, which seeds any missing keys
in `.env` with random values. Override by setting values manually before
install, or for cloud mode scaffold via `create-jazz --hosting hosted`.

| Variable                 | When       | Purpose                                                                       |
| ------------------------ | ---------- | ----------------------------------------------------------------------------- |
| `BETTER_AUTH_SECRET`     | always     | BetterAuth session signing. `src/lib/auth.ts` throws if missing.              |
| `PUBLIC_JAZZ_APP_ID`     | cloud only | Provisioned app ID. Unset in self-hosted dev — plugin injects it.             |
| `PUBLIC_JAZZ_SERVER_URL` | cloud only | Cloud sync URL (e.g. `https://v2.sync.jazz.tools`).                           |
| `JAZZ_ADMIN_SECRET`      | cloud only | Admin credential for schema pushes to the cloud.                              |
| `BACKEND_SECRET`         | always     | Persistent identity for the backend's Jazz account. Seeded by the scaffolder. |

In self-hosted mode (no cloud env vars), the `jazzSvelteKit` plugin spawns
a local Jazz dev server and supplies its own credentials.

## Deploying to production

`.env` is gitignored and not committed. Production deployments must
supply `BETTER_AUTH_SECRET` through your hosting provider's secret
management. The value must be consistent across restarts — rotating it
invalidates all existing Better Auth sessions.

Better Auth's in-memory adapter (`src/lib/auth.ts`) is a placeholder.
Swap it for a persistent database adapter before shipping, or users will
be wiped on every process restart.

## Known limitations

- **In-memory user store.** The Better Auth memory adapter keeps
  everything per-process, so HMR reloads, multi-worker deploys, and
  serverless invocations all reset state. Swap for a persistent adapter
  before shipping.

## Where to go next

- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.

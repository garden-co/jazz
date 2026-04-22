# next-betterauth

A Next.js starter for [Jazz](https://jazz.tools) with a strict
[Better Auth](https://better-auth.com) sign-in gate. The app is only
accessible to authenticated users.

## What this starter gives you

- A public sign-in / sign-up page, and a simple todo dashboard which uses Jazz
  for persistence, accessible to signed-in users.
- Better Auth handling email/password sign-up, sign-in, sign-out, and JWTs.
- A local Jazz dev server started automatically by the `withJazz` Next.js
  plugin in `next.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.

> [!TIP]
> If you want local-first onboarding with an optional upgrade, use `next-hybrid` instead. If you want no auth at all, use `next-localfirst`.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000), create an account,
and you'll land on `/dashboard` with a working todo list persisted via
Jazz. The `withJazz` plugin spawns a local Jazz dev server automatically;
set `BETTER_AUTH_SECRET` in `.env` before running (`openssl rand -base64
32` or scaffold via `create-jazz`).

## Architecture

```
app/
  layout.tsx                      ← plain root layout
  page.tsx                        ← public sign-in / sign-up form (redirects signed-in users to /dashboard)
  dashboard/
    layout.tsx                    ← auth guard + one-shot JWT fetch + JazzProvider
    page.tsx                      ← greeting, sign-out, <TodoWidget />
  api/auth/[...all]/route.ts      ← Better Auth catch-all handler
schema.ts                         ← Jazz app schema (todos table)
permissions.ts                    ← row-level access policy ($createdBy)
components/todo-widget.tsx        ← Jazz-powered todo list
lib/auth.ts                       ← Better Auth server config
lib/auth-client.ts                ← Better Auth React client
```

## How it works

Route protection is handled by two server components. `app/page.tsx` calls
`auth.api.getSession()` and redirects signed-in users to `/dashboard`.
`app/dashboard/layout.tsx` does the same check in the other direction,
redirecting signed-out users back to `/`.

`app/dashboard/layout.tsx` fetches a Better Auth JWT on each server render and
passes it to `<JazzProvider>`. Because the layout guard guarantees a
session on `/dashboard/*`, the provider is only mounted when the user is
authenticated — there's no anonymous fallback path to reason about.

`components/jazz-provider.tsx` mounts a `JwtRefresh` component inside the
provider that re-mints the JWT via `authClient.token()` whenever
`db.onAuthChanged` reports the token as expired, so long-lived sessions
won't silently drop to unauthenticated.

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

| Variable                      | When       | Purpose                                                             |
| ----------------------------- | ---------- | ------------------------------------------------------------------- |
| `BETTER_AUTH_SECRET`          | always     | BetterAuth session signing. `lib/auth.ts` throws loudly if missing. |
| `NEXT_PUBLIC_JAZZ_APP_ID`     | cloud only | Provisioned app ID. Unset in self-hosted dev — plugin injects it.   |
| `NEXT_PUBLIC_JAZZ_SERVER_URL` | cloud only | Cloud sync URL (e.g. `https://v2.sync.jazz.tools`).                 |
| `JAZZ_ADMIN_SECRET`           | cloud only | Admin credential for schema pushes to the cloud.                    |
| `BACKEND_SECRET`              | cloud only | Backend signing credential.                                         |

Generate a dev `BETTER_AUTH_SECRET` with `openssl rand -base64 32`. In
self-hosted mode (no cloud env vars), the `withJazz` plugin spawns a local
Jazz dev server and supplies its own credentials.

## Deploying to production

`.env` is gitignored and not committed. Production deployments must
supply `BETTER_AUTH_SECRET` through your hosting provider's secret
management. The value must be consistent across restarts — rotating it
invalidates all existing Better Auth sessions.

Better Auth's in-memory adapter (`lib/auth.ts`) is a placeholder.
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

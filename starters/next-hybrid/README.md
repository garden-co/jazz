# next-hybrid

A Next.js starter for [Jazz](https://jazz.tools) that combines local-first
authentication with a [Better Auth](https://better-auth.com) upgrade path. New
visitors can use the app without signing up. If they do choose to sign up,
their existing data is preserved by pinning the Better Auth account to their
local-first Jazz identity.

## What this starter gives you

- A working todo app that runs on first load, no sign-in required.
- An opt-in Better Auth sign-up / sign-in flow that upgrades the anonymous
  session to a named account without losing data.
- A local Jazz dev server started automatically by the `withJazz` Next.js
  plugin in `next.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000) and you'll land on the
app. `pnpm install` seeds `.env` with a random `BETTER_AUTH_SECRET` and
`BACKEND_SECRET`; the `withJazz` plugin spawns a local Jazz dev server
automatically.

## Architecture

```
app/
  layout.tsx                        ← root layout, mounts Providers
  page.tsx                          ← homepage (todo widget + auth nav)
components/jazz-provider.tsx          ← chooses anonymous or authenticated Jazz client
  api/auth/[...all]/route.ts        ← Better Auth catch-all handler
  signin/                           ← email/password sign-in form
  signup/                           ← email/password sign-up form
schema.ts                           ← Jazz app schema (todos table)
permissions.ts                      ← row-level access policy ($createdBy)
components/todo-widget.tsx          ← Jazz-powered todo list
lib/auth.ts                         ← Better Auth server config + identity proof hook
lib/auth-client.ts                  ← Better Auth React client
```

## How it works

`JazzProvider` in `components/jazz-provider.tsx` watches the Better Auth session via
`authClient.useSession()`. When there is no session, it calls
`BrowserAuthSecretStore.getOrCreateSecret()` and passes the secret to
`<JazzProvider>` as `secret`. When a session exists, it
fetches a Better Auth JWT and passes it to `<JazzProvider>` as
`jwtToken` instead.

### Identity continuity

The key design question: what happens to the todos a user created
anonymously when they sign up for a named account?

They carry over automatically. Here is how:

1. At sign-up, the client calls `db.getLocalFirstIdentityProof(...)` to
   mint a short-lived proof token that cryptographically asserts the
   client's current Jazz user id.
2. The signup form posts this token alongside email/password to Better
   Auth.
3. `lib/auth.ts` intercepts the `/sign-up/email` request via a
   `hooks.before` middleware, calls `verifyLocalFirstIdentityProof` (from
   `jazz-napi`), and extracts the verified Jazz user id.
4. `databaseHooks.user.create.before` pins the new Better Auth `user.id`
   to that Jazz user id instead of generating a fresh one.
5. All subsequent JWTs carry `sub: <jazz-user-id>`, so the server sees
   the same principal that created the anonymous todos.

`components/jazz-provider.tsx` also mounts a `JwtRefresh` component inside
the provider that re-mints the JWT via `authClient.token()` whenever
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

`pnpm install` runs `scripts/ensure-env.js`, which seeds any missing keys
in `.env` with random values. Override by setting values manually before
install, or for cloud mode scaffold via `create-jazz --hosting hosted`.

| Variable                      | When       | Purpose                                                                       |
| ----------------------------- | ---------- | ----------------------------------------------------------------------------- |
| `BETTER_AUTH_SECRET`          | always     | BetterAuth session signing. `lib/auth.ts` throws if missing.                  |
| `NEXT_PUBLIC_JAZZ_APP_ID`     | cloud only | Provisioned app ID. Unset in self-hosted dev — plugin injects it.             |
| `NEXT_PUBLIC_JAZZ_SERVER_URL` | cloud only | Cloud sync URL (e.g. `https://v2.sync.jazz.tools`).                           |
| `JAZZ_ADMIN_SECRET`           | cloud only | Admin credential for schema pushes to the cloud.                              |
| `BACKEND_SECRET`              | always     | Persistent identity for the backend's Jazz account. Seeded by the scaffolder. |

In self-hosted mode (no cloud env vars), the `withJazz` plugin spawns a
local Jazz dev server and supplies its own credentials.

## Deploying to production

The Jazz cloud server requires `--allow-local-first-auth` explicitly in
production: `jazz-tools server <APP_ID> --allow-local-first-auth`.
Without it, anonymous local-first connections will receive auth errors.

`.env` is gitignored and not committed. Production deployments must
supply `BETTER_AUTH_SECRET` through your hosting provider's secret
management. The value must be consistent across restarts, as rotating it
invalidates all existing Better Auth sessions.

Better Auth's in-memory adapter (`lib/auth.ts`) is a placeholder.
Swap it for a persistent database adapter before shipping, or users will
be wiped on every process restart.

## Known limitations

### Cross-device sign-in orphans local-first data

Each Better Auth account can only be linked to a single local-first account.

For example:

1. A user creates some local-first data on device A
2. They switch to device B and create some more data
3. The user signs up using Better Auth on device A
4. The user's local-first data from device A will be accessible from their Better Auth account
5. The user goes to device B and signs in to Better Auth. The user will see their data from device A, _but their existing data on device B will become inaccessible_.

## Where to go next

- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.

# sveltekit-hybrid

A SvelteKit starter for [Jazz](https://jazz.tools) that combines
local-first data with a [Better Auth](https://better-auth.com) upgrade
path. New visitors use the app anonymously, and signing up preserves
their existing data by pinning the Better Auth account to the browser's
Jazz identity.

## What this starter gives you

- A working todo app that runs on first load, no sign-in required.
- An opt-in Better Auth sign-up / sign-in flow that upgrades the anonymous
  session to a named account without losing data.
- A local Jazz dev server started automatically by the `jazzSvelteKit`
  Vite plugin in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the session that created it.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) and you'll land on
the app. Set `BETTER_AUTH_SECRET` in `.env` before running
(`openssl rand -base64 32` or scaffold via `create-jazz`).

## Architecture

```
src/
  app.html                         ← HTML shell
  app.css                          ← global styles
  app.d.ts                         ← SvelteKit types
  hooks.server.ts                  ← Better Auth handler + trusted origin
  lib/
    schema.ts                      ← Jazz app schema (todos table)
    permissions.ts                 ← row-level access policy ($createdBy)
    TodoWidget.svelte              ← Jazz-powered todo list
    auth.ts                        ← Better Auth server config + identity proof hook
    auth-client.ts                 ← Better Auth Svelte client
  routes/
    +layout.svelte                 ← switches between anonymous and JWT Jazz clients
    +page.svelte                   ← homepage (todo widget + auth nav)
    signup/+page.svelte             ← email/password sign-up form
    signin/+page.svelte             ← email/password sign-in form
```

## How it works

`src/routes/+layout.svelte` watches the Better Auth session via
`authClient.useSession()`. When there is no session, it calls
`BrowserAuthSecretStore.getOrCreateSecret()` and passes the secret to
`createJazzClient` as `secret`. When a session exists, it
fetches a Better Auth JWT and creates the client with `jwtToken` instead.

The layout uses a `clientAuth` gate so the `<JazzSvelteProvider>` only
renders when the active client matches the current session state. This
prevents a race where the UI would briefly interact with the stale
anonymous client during a sign-up → authenticated transition.

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
   the same principal that created the anonymous todos. Same Jazz id,
   same data.

After `authClient.signUp.email` resolves, the signup page calls
`authClient.getSession()` and waits for it before calling `goto("/")` —
this ensures the layout observes `authenticated=true` before the home
page mounts, so it builds the authenticated client directly instead of
briefly rendering the stale anonymous one.

The layout also installs a `db.onAuthChanged` listener that re-mints the
JWT via `authClient.token()` whenever Better Auth reports the token as
expired — long-lived sessions won't silently drop to unauthenticated.

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

| Variable                 | When       | Purpose                                                              |
| ------------------------ | ---------- | -------------------------------------------------------------------- |
| `BETTER_AUTH_SECRET`     | always     | BetterAuth session signing. `src/lib/auth.ts` throws if missing.     |
| `PUBLIC_JAZZ_APP_ID`     | cloud only | Provisioned app ID. Unset in self-hosted dev — plugin injects it.    |
| `PUBLIC_JAZZ_SERVER_URL` | cloud only | Cloud sync URL (e.g. `https://prod.v2.aws.cloud.jazz.tools/apps/…`). |
| `JAZZ_ADMIN_SECRET`      | cloud only | Admin credential for schema pushes to the cloud.                     |
| `BACKEND_SECRET`         | cloud only | Backend signing credential.                                          |

Generate a dev `BETTER_AUTH_SECRET` with `openssl rand -base64 32`. In
self-hosted mode (no cloud env vars), the `jazzSvelteKit` plugin spawns a
local Jazz dev server and supplies its own credentials.

## Deploying to production

The Jazz cloud server requires `--allow-local-first-auth` explicitly in
production. In development this flag is on by default; in production you
must pass it: `jazz-tools server <APP_ID> --allow-local-first-auth`.
Without it, anonymous local-first connections will receive auth errors.

`.env` is gitignored and not committed. Production deployments must
supply `BETTER_AUTH_SECRET` through your hosting provider's secret
management. The value must be consistent across restarts — rotating it
invalidates all existing Better Auth sessions.

Better Auth's in-memory adapter (`lib/auth.ts`) is a placeholder.
Swap it for a persistent database adapter before shipping, or users will
be wiped on every process restart.

## Known limitations

### Cross-device sign-in orphans anonymous data

If a user signs up on device A (which pins their Better Auth account to
Jazz user id `X`), then opens the app fresh on device B (which creates a
new anonymous Jazz user id `Y`), creates todos on device B, and then
signs in on device B — the JWT carries `sub: X`. Any rows created under
`Y` become orphaned: they remain on the server but are inaccessible to
the signed-in principal, since policies match on `$createdBy`.

This is architectural: the current local-first auth model pins exactly
one Jazz id per Better Auth account at sign-up. It is not trivially
fixable inside a starter.

## Removing BetterAuth

If you don't need named accounts, use the `sveltekit-localfirst` starter
instead — it's this starter with all the auth code stripped out, and
it's the supported path.

If you already scaffolded this starter and want to strip BetterAuth
manually:

1. Delete `src/hooks.server.ts`, `lib/auth.ts`,
   `src/lib/auth-client.ts`, `src/routes/signin/`, and
   `src/routes/signup/`.
2. In `src/routes/+layout.svelte`, remove the `authClient`, `session`,
   `clientAuth`, and `ready` logic — reduce the effect to: resolve
   `BrowserAuthSecretStore.getOrCreateSecret()`, build a `DbConfig` with
   `auth: { localFirstSecret: secret }`, and call `createJazzClient`.
3. In `src/routes/+page.svelte`, remove the auth-nav block and the
   `handleSignOut` function — the page renders just the header and the
   `<TodoWidget />`.
4. In `package.json`, remove `better-auth` and `jazz-napi` from
   `dependencies`.

## Where to go next

- `docs/content/docs/auth/local-first-auth.mdx` — full explanation of the
  local-first auth model, `BrowserAuthSecretStore`, and
  `getLocalFirstIdentityProof`.
- `docs/content/docs/authentication.mdx` — overview of all Jazz auth modes.
- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.

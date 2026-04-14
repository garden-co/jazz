# sveltekit-betterauth

A minimal SvelteKit starter that combines [Jazz](https://jazz.tools) with
[Better Auth](https://better-auth.com) — email/password sign-in, a gated
dashboard, and a Jazz-backed todo list scoped to the signed-in user.

This is the SvelteKit twin of the `next-betterauth` starter; the architecture
is identical where possible.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173), create an account, and
you'll land on `/dashboard` with a working todo list persisted via Jazz.

No `.env` setup required — Better Auth uses a hardcoded development secret
and the Jazz dev server is started automatically by the `jazzSvelteKit`
Vite plugin in `vite.config.ts`.

## Architecture

```
src/
  app.html                        ← HTML shell
  app.css                         ← global styles
  app.d.ts                        ← SvelteKit types
  hooks.server.ts                 ← Better Auth handler + cookie-based route gate
  lib/
    auth.ts                       ← Better Auth server config
    auth-client.ts                ← Better Auth Svelte client
    schema.ts                     ← Jazz app schema (todos table)
    TodoWidget.svelte             ← Jazz-powered todo list
  routes/
    +layout.svelte                ← plain root layout (imports app.css)
    +page.svelte                  ← public sign-in / sign-up form
    dashboard/
      +layout.svelte              ← one-shot JWT fetch + JazzSvelteProvider
      +page.svelte                ← greeting, sign-out, <TodoWidget/>
```

### Auth flow

- **Better Auth** handles sign-up, sign-in, sign-out, and JWT issuing. All
  `/api/auth/*` traffic is routed through `svelteKitHandler` inside
  `hooks.server.ts`.
- **`hooks.server.ts`** also checks the session cookie on every request and
  bidirectionally redirects: `/` to `/dashboard` for signed-in users, and
  `/dashboard/*` back to `/` for signed-out users. This uses
  `getSessionCookie` — a cheap cookie-presence check, not a full DB read.
- **`src/lib/auth.ts`** uses Better Auth's in-memory adapter, which means
  your users are wiped on every dev-server restart. Swap in a real database
  adapter when you're ready to persist.

### Jazz integration

- The `jazzSvelteKit` plugin in `vite.config.ts` starts a local Jazz dev
  server on `vite dev`, pushes `schema.ts` to it, and injects
  `PUBLIC_JAZZ_APP_ID` + `PUBLIC_JAZZ_SERVER_URL` for the client to read.
- `src/routes/dashboard/+layout.svelte` fetches a Better Auth JWT once on
  mount and passes it to `<JazzSvelteProvider>`. Because the hook guarantees
  a session on `/dashboard/*`, the provider is only mounted when the user
  is authenticated.
- `src/lib/TodoWidget.svelte` shows the typical Jazz usage pattern: `getDb()`
  for mutations, `QuerySubscription` for reactive queries, `getSession()`
  for the authenticated user id.

### Extending the schema

Edit `schema.ts` to add tables. The Jazz dev server watches the file and
republishes the schema on change, so no restart is needed.

```ts
const schema = {
  todos: s.table({ title: s.string(), done: s.boolean(), ownerId: s.string() }),
  projects: s.table({ name: s.string(), ownerId: s.string() }),
};
```

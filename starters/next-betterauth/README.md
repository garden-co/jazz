# next-betterauth

A minimal Next.js starter that combines [Jazz](https://jazz.tools) with
[Better Auth](https://better-auth.com) — email/password sign-in, a gated
dashboard, and a Jazz-backed todo list scoped to the signed-in user.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000), create an account, and
you'll land on `/dashboard` with a working todo list persisted via Jazz.

No `.env` setup required — Better Auth uses a hardcoded development secret
and the Jazz dev server is started automatically by the `withJazz` plugin in
`next.config.ts`.

## Architecture

```
app/
  layout.tsx                      ← plain root layout
  page.tsx                        ← public sign-in / sign-up form
  dashboard/
    layout.tsx                    ← one-shot JWT fetch + JazzProvider
    page.tsx                      ← greeting, sign-out, <TodoWidget/>
  api/auth/[...all]/route.ts      ← Better Auth handler
middleware.ts                     ← cookie-based route gate
schema.ts                         ← Jazz app schema (todos table)
src/
  lib/auth.ts                     ← Better Auth server config
  lib/auth-client.ts              ← Better Auth React client
  components/todo-widget.tsx      ← Jazz-powered todo list
```

### Auth flow

- **Better Auth** handles sign-up, sign-in, sign-out, and JWT issuing via the
  catch-all `/api/auth/[...all]` route.
- **`middleware.ts`** checks the session cookie on every request. It redirects
  authenticated users from `/` to `/dashboard` and unauthenticated users from
  `/dashboard/*` to `/`. This uses `getSessionCookie` — a cheap cookie-presence
  check, not a full DB read.
- **`src/lib/auth.ts`** uses Better Auth's in-memory adapter, which means your
  users are wiped on every dev-server restart. Swap in a real database adapter
  when you're ready to persist.

### Jazz integration

- The `withJazz` plugin in `next.config.ts` starts a local Jazz dev server on
  `next dev`, pushes `schema.ts` to it, and injects the necessary env vars.
- `app/dashboard/layout.tsx` fetches a Better Auth JWT once on mount and
  passes it to `<JazzProvider>`. Because the proxy guarantees a session on
  `/dashboard/*`, the provider is only mounted when the user is authenticated.
- `src/components/todo-widget.tsx` shows the typical Jazz usage pattern:
  `useDb()` for mutations, `useAll()` for reactive queries, `useSession()` for
  the authenticated user id.

### Extending the schema

Edit `schema.ts` to add tables. The Jazz dev server watches the file and
republishes the schema on change, so no restart is needed.

```ts
const schema = {
  todos: s.table({ title: s.string(), done: s.boolean() }),
  projects: s.table({ name: s.string() }),
};
```

Row ownership is enforced by `permissions.ts` via the `$createdBy` predicate,
so you don't need an explicit `ownerId` column — Jazz records the creating
session on every row and the permission policy scopes reads/writes to it.

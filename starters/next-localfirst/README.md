# next-localfirst

A minimal Next.js starter for [Jazz](https://jazz.tools) with a pure
local-first todo app. Users' data persists under a per-device anonymous Jazz
identity.

## What this starter gives you

- A working todo app that runs on first load, no configuration required.
- A local Jazz dev server started automatically by the `withJazz` Next.js
  plugin in `next.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.
- Zero auth code to wade through while you get your bearings.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000) and you'll land on the
app. No `.env` setup required — the Jazz dev server and its env vars are
injected automatically by the `withJazz` plugin.

## Architecture

```
app/
  layout.tsx                     ← root layout, mounts the Jazz provider
  page.tsx                       ← homepage (header + todo widget)
  providers.tsx                  ← LocalFirstProvider (per-device secret)
  globals.css
schema.ts                        ← Jazz app schema (todos table)
permissions.ts                   ← row-level access policy ($createdBy)
components/todo-widget.tsx       ← Jazz-powered todo list
```

## How it works

Every browser gets its own Ed25519 secret, generated and stored by
`BrowserAuthSecretStore` on first load. That secret becomes the identity
Jazz uses for all subsequent writes. `LocalFirstProvider` in
`app/providers.tsx` does exactly one thing: call
`BrowserAuthSecretStore.getOrCreateSecret()` and hand the result to
`<JazzProvider>` as `auth.localFirstSecret`.

Data syncs to the Jazz server under that anonymous identity. There is no
concept of a user account, no sign-in, no sign-out — the device _is_ the
account.

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

None required in development. `NEXT_PUBLIC_JAZZ_APP_ID` and
`NEXT_PUBLIC_JAZZ_SERVER_URL` are injected at runtime by the `withJazz`
Next.js plugin on `next dev`. For production, set them explicitly via your
hosting provider.

## Deploying to production

The Jazz cloud server requires `--allow-local-first-auth` explicitly in
production: `jazz-tools server <APP_ID> --allow-local-first-auth`.
Without it, anonymous local-first connections will receive auth errors.

## Known limitations

- **One device per user.** The secret lives in browser storage; clearing
  site data wipes the identity and the user starts fresh. There is no
  account portability between devices or browsers.
- **No account recovery.** If a user loses their device, their data is
  gone. When those constraints matter, use the `next-hybrid`
  starter instead.

## Where to go next

- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.

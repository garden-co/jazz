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
app. No `.env` setup required for self-hosted mode — the `withJazz` plugin
spawns a local Jazz dev server and injects its credentials at startup.

To run against Jazz Cloud instead, scaffold via `create-jazz` (which writes
the four cloud env vars for you) or fill in `.env` by hand — see
**Environment variables** below.

## Architecture

```
app/
  layout.tsx                   ← root layout, mounts <JazzProvider>
  page.tsx                     ← homepage (header + todo widget + backup UI)
  globals.css
components/
  jazz-provider.tsx            ← LocalFirstProvider (per-device secret)
  todo-widget.tsx              ← Jazz-powered todo list
  auth-backup.tsx              ← recovery phrase + passkey backup/restore
schema.ts                      ← Jazz app schema (todos table)
permissions.ts                 ← row-level access policy ($createdBy)
```

## How it works

Every browser gets its own Ed25519 secret, generated and stored by
`BrowserAuthSecretStore` on first load. That secret becomes the identity
Jazz uses for all subsequent writes. `LocalFirstProvider` in
`components/jazz-provider.tsx` does exactly one thing: call
`BrowserAuthSecretStore.getOrCreateSecret()` and hand the result to
`<JazzProvider>` as `secret`.

Data syncs to the Jazz server under that anonymous identity. There is no
concept of a user account, no sign-in, no sign-out — the device _is_ the
account. `components/auth-backup.tsx` surfaces a recovery phrase + passkey
UI so users can back up and restore that identity across devices.

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

| Variable                      | When       | Source                                                |
| ----------------------------- | ---------- | ----------------------------------------------------- |
| `NEXT_PUBLIC_JAZZ_APP_ID`     | cloud only | scaffolder (`create-jazz --hosting hosted`) or manual |
| `NEXT_PUBLIC_JAZZ_SERVER_URL` | cloud only | scaffolder or manual                                  |
| `JAZZ_ADMIN_SECRET`           | cloud only | scaffolder or manual                                  |
| `BACKEND_SECRET`              | cloud only | scaffolder or manual                                  |

Leave all four unset for self-hosted mode — the `withJazz` plugin spawns a
local dev server and injects its own ephemeral credentials. For cloud mode,
either scaffold via `create-jazz --hosting hosted` (writes `.env` for you)
or provision an app at https://v2.dashboard.jazz.tools and paste the four
values into `.env`.

## Deploying to production

For cloud-hosted deployments, set the four env vars above in your hosting
provider and your app will sync against Jazz Cloud.

For self-hosted deployments you need to run your own Jazz server. The
server requires `--allow-local-first-auth` explicitly in production:
`jazz-tools server <APP_ID> --allow-local-first-auth`. Without it,
anonymous local-first connections will receive auth errors.

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

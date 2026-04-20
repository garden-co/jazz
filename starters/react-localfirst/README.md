# react-selfhosted-localfirst

A minimal Vite + React starter for [Jazz](https://jazz.tools) with a pure
local-first todo app. Users' data persists under a per-device anonymous Jazz
identity.

## What this starter gives you

- A working todo app that runs on first load, no configuration required.
- A local Jazz dev server started automatically by the `jazzPlugin` Vite
  plugin in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.
- Zero auth code to wade through while you get your bearings.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) and you'll land on the
app. No `.env` setup required — the Jazz dev server and its env vars are
injected automatically by the `jazzPlugin` Vite plugin.

## Architecture

```
src/
  main.tsx                       ← app entry, mounts the Jazz provider
  App.tsx                        ← homepage (header + todo widget)
  todo-widget.tsx                ← Jazz-powered todo list
  App.css
schema.ts                        ← Jazz app schema (todos table)
permissions.ts                   ← row-level access policy ($createdBy)
```

## How it works

Every browser gets its own Ed25519 secret, generated and stored by
`BrowserAuthSecretStore` on first load. That secret becomes the identity
Jazz uses for all subsequent writes. `LocalFirstProvider` in
`src/main.tsx` does exactly one thing: call
`BrowserAuthSecretStore.getOrCreateSecret()` and hand the result to
`<JazzProvider>` as `secret`.

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

## Local development

No `.env` setup is required for the first run. The `jazzPlugin` Vite plugin
starts a local Jazz dev server and writes `VITE_JAZZ_APP_ID` and
`VITE_JAZZ_SERVER_URL` into `.env` on the first `pnpm dev`. On the
second run (and every run after), Vite picks them up from `.env`
automatically.

If you prefer to wire things up front, create `.env` before running
`pnpm dev`:

```
VITE_JAZZ_APP_ID=local-dev
VITE_JAZZ_SERVER_URL=http://localhost:4433
```

## Environment variables

| Variable               | When       | Source                                                |
| ---------------------- | ---------- | ----------------------------------------------------- |
| `VITE_JAZZ_APP_ID`     | cloud only | scaffolder (`create-jazz --hosting hosted`) or manual |
| `VITE_JAZZ_SERVER_URL` | cloud only | scaffolder or manual                                  |
| `JAZZ_ADMIN_SECRET`    | cloud only | scaffolder or manual                                  |
| `BACKEND_SECRET`       | cloud only | scaffolder or manual                                  |

Leave all four unset for self-hosted mode — the `jazzPlugin` Vite plugin
spawns a local Jazz dev server and writes `VITE_JAZZ_APP_ID` /
`VITE_JAZZ_SERVER_URL` into `.env` on first `pnpm dev`. For cloud mode,
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
  gone. When those constraints matter, use the `react-selfhosted-hybrid`
  starter instead.

## Where to go next

- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.

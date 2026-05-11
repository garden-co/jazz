# ts-localfirst

A minimal Vite + plain-TypeScript starter for [Jazz](https://jazz.tools) with
a pure local-first todo app. No UI framework — just `document.createElement`
and direct DOM updates inside the Jazz subscription callback.

## What this starter gives you

- A working todo app that runs on first load, no configuration required.
- A local Jazz dev server started automatically by the `jazzPlugin` Vite
  plugin in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.
- Zero auth code and zero framework abstractions to wade through while you
  get your bearings.

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
  main.ts                        ← app entry, boots Jazz and mounts widgets
  app.ts                         ← homepage shell (header + slots)
  todo-widget.ts                 ← Jazz-powered todo list (direct DOM)
  auth-backup.ts                 ← recovery phrase + passkey controls
  app.css
schema.ts                        ← Jazz app schema (todos table)
permissions.ts                   ← row-level access policy ($createdBy)
```

## How it works

Every browser gets its own Ed25519 secret, generated and stored by
`BrowserAuthSecretStore` on first load. `src/main.ts` calls
`BrowserAuthSecretStore.getOrCreateSecret()` and hands the result to
`createDb({ appId, serverUrl, secret })` — no React provider, no hooks.

Each widget receives the `Db` handle and wires its DOM straight to it:

```ts
return db.subscribeAll(app.todos, (delta) => {
  list.replaceChildren(...delta.all.map(renderRow));
});
```

The subscription callback fires on every change with the full materialised
result set in `delta.all`. The widget rebuilds its `<ul>` on each tick —
simple, fast enough for the kinds of lists a starter needs, and easy to
swap for per-row patches via `delta.delta` if you ever need them.

Data syncs to the Jazz server under the device's anonymous identity. There
is no concept of a user account, no sign-in, no sign-out — the device _is_
the account.

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
  gone. When those constraints matter, use the `ts-hybrid` starter instead.

## Where to go next

- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.
- `src/todo-widget.ts` — the canonical pattern for a Jazz-backed widget
  without a UI framework.

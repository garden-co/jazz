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

`src/main.ts` prepares an account manager, restores the selected opaque
`AccountHandle` or creates a local-first account, then passes that handle to
`createJazzClient`. The backup controls use the manager's export and restore
APIs, so raw credentials never become application state.

Each widget receives the `Db` handle and wires its DOM straight to it:

```ts
return db.subscribe(app.todos, (todos) => {
  list.replaceChildren(...todos.map(renderRow));
});
```

The subscription callback fires on every change with the full materialised
result set. The widget rebuilds its `<ul>` on each tick — simple and fast
enough for the kinds of lists a starter needs.

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
spawns a local Jazz dev server, persists `VITE_JAZZ_APP_ID` in `.env`,
and injects `VITE_JAZZ_SERVER_URL` while running `pnpm dev`. For cloud mode,
either scaffold via `create-jazz --hosting hosted` (writes `.env` for you)
or provision an app at https://v2.dashboard.jazz.tools and paste the four
values into `.env`.

## Deploying to production

For either hosting mode, set `VITE_JAZZ_APP_ID` and `VITE_JAZZ_SERVER_URL`
before running `pnpm build`. Vite embeds these values in the browser bundle;
setting them only when starting `pnpm preview` does not configure an existing
build. The dev plugin does not supply a production server URL.

For cloud-hosted deployments, set the four env vars above in your hosting
provider and your app will sync against Jazz Cloud.

For self-hosted deployments you need to run your own Jazz server. The
server requires `--allow-local-first-auth` explicitly in production:
`jazz-tools server <APP_ID> --allow-local-first-auth`. Without it,
anonymous local-first connections will receive auth errors.

## Known limitations

- **Back up before clearing browser storage.** The selected account is local
  to this browser until the user saves the recovery phrase or passkey backup.

## Where to go next

- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.
- `src/todo-widget.ts` — the canonical pattern for a Jazz-backed widget
  without a UI framework.

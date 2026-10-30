# ts-effect-localfirst

A minimal Vite + TypeScript starter for [Jazz](https://jazz.tools) with a pure
local-first todo app written with [Effect](https://effect.website) v4. No UI
framework: reads, writes and the live todo list are Effects and Streams on the
`Jazz` service from `jazz-tools/effect`, wired straight to the DOM.

## What this starter gives you

- A working todo app that runs on first load, no configuration required.
- A local Jazz dev server started automatically by the `jazzPlugin` Vite
  plugin in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.
- Jazz as an Effect service: typed errors (`JazzError`, `JazzWriteRejected`),
  live queries as Streams and writes that complete at the durability you ask
  for.
- Zero auth code and zero UI framework abstractions to wade through while you
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
  todo-widget.ts                 ← Effect todo list: Jazz Effects + direct DOM
  auth-backup.ts                 ← recovery phrase + passkey controls
  app.css
schema.ts                        ← Jazz app schema (todos table)
permissions.ts                   ← row-level access policy ($createdBy)
```

## How it works

`createJazzSession` receives the configuration once with `initial: "local-first"`. It restores a usable saved account or creates a local-first account and owns the client lifecycle. Recovery controls call `restoreLocalFirst`; the session waits for sync before replacing the client and preserves a usable account after a failed operation.

`app.ts` wraps the session's database with `Jazz.fromDb(db)`, which gives the
todo widget the Effect `Jazz` service. The widget's Jazz code only depends on
that service, so the same Effects run in a test or on a server:

```ts
export const addTodo = (title: string) =>
  Jazz.use((jazz) => jazz.insert(app.todos, { title, done: false }, { wait: "local" }));

export const todos = Stream.unwrap(Jazz.useSync((jazz) => jazz.stream(app.todos)));
```

`addTodo` completes once the todo is durable on this device and fails with a
typed `JazzError` otherwise. `todos` emits the full result set whenever it
changes, and the widget rebuilds its `<ul>` on each snapshot. Writes that are
later rejected on their way to the server arrive on `jazz.mutationErrors`,
which the widget shows as "Saved locally; sync failed". Unmounting the widget
interrupts its fiber, which removes the live query.

Data syncs to the Jazz server under the device's anonymous identity. There
is no concept of a user account, no sign-in, no sign-out — the device _is_
the account.

## Extending the schema

Edit `schema.ts` to add tables. The Jazz dev server watches the file and
republishes the schema on change — no restart needed.

```ts
const schema = {
  todos: s.table({ title: s.string(), done: s.boolean() }, {}),
  projects: s.table({ name: s.string() }, {}),
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
- `src/todo-widget.ts` — Jazz Effects and Streams driving plain DOM.
- The [Effect guide](https://jazz.tools/docs/recipes/effect) for transactions,
  durability waits and server-side use with `jazz-tools/effect/backend`.
- `ts-effect-betterauth` — the same client with sign-in and an Effect HTTP
  server that acts as the signed-in user.

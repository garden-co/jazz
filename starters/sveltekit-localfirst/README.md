# sveltekit-localfirst

A minimal SvelteKit starter for [Jazz](https://jazz.tools) with a pure
local-first todo app — no accounts, no sign-in wall, data persists under a
per-device anonymous Jazz identity. The simplest possible Jazz starter.

## What this starter gives you

- A working todo app that runs on first load, no configuration required.
- A local Jazz dev server started automatically by the `jazzSvelteKit`
  Vite plugin in `vite.config.ts`.
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
injected automatically by the `jazzSvelteKit` plugin.

## Architecture

```
src/
  app.html                       ← HTML shell
  app.css                        ← global styles
  app.d.ts                       ← SvelteKit types
  lib/
    schema.ts                    ← Jazz app schema (todos table)
    permissions.ts               ← row-level access policy ($createdBy)
    TodoWidget.svelte            ← Jazz-powered todo list
  routes/
    +layout.svelte               ← mounts the Jazz provider (per-device secret)
    +page.svelte                 ← homepage (header + todo widget)
```

## How it works

Every browser gets its own Ed25519 secret, generated and stored by
`BrowserAuthSecretStore` on first load. That secret becomes the identity
Jazz uses for all subsequent writes. `src/routes/+layout.svelte` does
exactly one thing: call `BrowserAuthSecretStore.getOrCreateSecret()` and
hand the result to `createJazzClient` as `secret`.

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

| Variable                 | When       | Source                                                |
| ------------------------ | ---------- | ----------------------------------------------------- |
| `PUBLIC_JAZZ_APP_ID`     | cloud only | scaffolder (`create-jazz --hosting hosted`) or manual |
| `PUBLIC_JAZZ_SERVER_URL` | cloud only | scaffolder or manual                                  |
| `JAZZ_ADMIN_SECRET`      | cloud only | scaffolder or manual                                  |
| `BACKEND_SECRET`         | cloud only | scaffolder or manual                                  |

Leave all four unset for self-hosted mode — the `jazzSvelteKit` plugin
spawns a local Jazz dev server and injects its own credentials. For cloud
mode, either scaffold via `create-jazz --hosting hosted` (writes `.env` for
you) or provision an app at https://v2.dashboard.jazz.tools and paste the
four values into `.env`.

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
  gone. When those constraints matter, use the `sveltekit-hybrid`
  starter instead.

## Adding BetterAuth later

If you later want named accounts with an upgrade path that preserves
existing anonymous data, scaffold the `sveltekit-hybrid` starter
in a sibling directory and copy its `src/lib/auth.ts`,
`src/lib/auth-client.ts`, `src/hooks.server.ts`, `src/routes/signin/`, and
`src/routes/signup/` into this project. Rework `src/routes/+layout.svelte`
to switch between anonymous and JWT-authenticated clients based on the
Better Auth session (see the `-auth` variant for the exact pattern), add
`better-auth` and `jazz-napi` as dependencies, and set
`BETTER_AUTH_SECRET` in `src/lib/auth.ts` or your environment.

## Where to go next

- `docs/content/docs/auth/local-first-auth.mdx` — full explanation of the
  local-first auth model and `BrowserAuthSecretStore`.
- `docs/content/docs/authentication.mdx` — overview of all Jazz auth modes.
- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.

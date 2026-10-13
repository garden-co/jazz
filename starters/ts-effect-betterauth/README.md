# ts-effect-betterauth

A Vite + TypeScript starter for [Jazz](https://jazz.tools) written with
[Effect](https://effect.website) v4 on both sides, with
[Better Auth](https://better-auth.com) email/password sign-up gating all
access. In the browser, the todo list uses the `Jazz` service from
`jazz-tools/effect`. On the server, an `effect/http` route uses
`jazz-tools/effect/backend` to act on Jazz as the signed-in user. No UI
framework.

The server host is Hono, which serves Better Auth, the built app and the SPA
fallback. The Effect routes are an `effect/http` `HttpRouter`, mounted under
`/api/todos/*` with `HttpRouter.toWebHandler`, so you can grow them into a
standalone `effect/http` server without rewriting them.

## What this starter gives you

- Email/password sign-up and sign-in required upfront — no anonymous access.
- Better Auth handling sign-up, sign-in, sign-out, and short-lived JWTs.
- Jazz as an Effect service in the browser: typed errors, live queries as
  Streams and writes that complete at the durability you ask for.
- An Effect HTTP route (`POST /api/todos/clear-completed`) that reads and
  deletes the requesting user's todos with that user's permissions, through
  `JazzBackend.forCurrentRequest()`.
- A local Jazz dev server started automatically by the `jazzPlugin` Vite plugin
  in `vite.config.ts`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.

> [!TIP]
> If you want no auth and no server code, use `ts-effect-localfirst`.

## Getting started

```bash
pnpm install
pnpm dev
```

Open [http://localhost:3001](http://localhost:3001). The app shows a sign-up
form on first load; once you create an account it switches to the todo list.
Set `BETTER_AUTH_SECRET` in `.env` before running (`openssl rand -base64 32`
or scaffold via `create-jazz`).

## Architecture

```
src/
  main.ts                        ← app entry; creates the observable Jazz app immediately
  app.ts                         ← shell renderer: sign-in form vs todo dashboard
  sign-in-form.ts                ← combined sign-in/sign-up form (mode toggle)
  todo-widget.ts                 ← Effect todo list: Jazz Effects + direct DOM
  clear-completed.ts             ← Effect that calls the server route as this user
  auth-client.ts                 ← Better Auth vanilla client
  app.css
server/
  jazz-api.ts                    ← Effect routes on a shared JazzBackend session
  app.ts                         ← Hono host: Better Auth, Effect routes, static files
  auth.ts                        ← Better Auth server config
  dev.ts                         ← `pnpm dev`: Vite in middleware mode + the API on 3001
  index.ts                       ← production entry; listens on port 3001
schema.ts                        ← Jazz app schema (todos table)
permissions.ts                   ← row-level access policy ($createdBy)
scripts/
  ensure-env.js                  ← generates BETTER_AUTH_SECRET in .env
```

## How it works

One Node process on port 3001 serves everything, in development and in
production:

- `/api/auth/*`: Better Auth (sign-up, sign-in, token, JWKS).
- `/api/todos/*`: the Effect routes in `server/jazz-api.ts`.
- Everything else: the client. In development `server/dev.ts` runs Vite in
  middleware mode, so the `jazzPlugin` starts the local Jazz server in the same
  process and the API reads its settings from `process.env`. In production the
  built client is served from `dist/`.

The entry point creates one `createJazzApp({ appId, serverUrl, auth: betterAuth(authClient) })`.
The DOM shell observes its snapshot for startup, signed-out, ready, and error views.
It detaches old todo subscriptions before acknowledging each snapshot through its
consumer lease, and releases that lease before disposing the app on page exit.
A separate profile observer updates only the greeting; it does not drive Jazz identity.
Sign-up and sign-in forms only call Better Auth. The connection watches initial
hydration, login, signup, restoration, and logout; it atomically logs in or
creates the Jazz account with `loginOrRegisterJWT`. Repeated notifications for
the same provider identity do not replace the client. Jazz requests fresh JWTs
from `/token` when credentials expire.

Sign-out goes through the app, which flushes Jazz before Better Auth
revokes credentials. Failures stay visible with a retry action.

Once signed in, `app.ts` wraps the database with `Jazz.fromDb(db)` and hands
the todo widget the Effect `Jazz` service. Its operations only depend on that
service:

```ts
export const addTodo = (title: string) =>
  Jazz.use((jazz) => jazz.insert(app.todos, { title, done: false }, { wait: "local" }));
```

The server opens one `JazzBackend` session for its lifetime. Each route is an
Effect that reads `Jazz`, and `JazzBackend.forCurrentRequest()` provides it as
the user whose bearer token sent the request:

```ts
export const clearCompletedTodos = Effect.gen(function* () {
  const jazz = yield* Jazz;
  const done = yield* jazz.all(app.todos.where({ done: true }), { tier: "remote" });
  yield* Effect.forEach(done, (todo) => jazz.delete(app.todos, todo.id, { wait: "global" }));
  return done.length;
});
```

The browser calls it with a Better Auth JWT from `/api/auth/token`, the same
kind of token Jazz sync uses. The deletions sync back into the live list.
Because `clearCompletedTodos` only needs `Jazz`, `server/jazz-api.test.ts` runs
it against a fake service without any server.

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

## Local development

`pnpm dev` runs `server/dev.ts` under `tsx watch`. If you change `server/`, the
process restarts (and with it Vite and the local Jazz server). If you change
`src/`, Vite hot-reloads.

## Environment variables

| Variable               | When       | Source                                                |
| ---------------------- | ---------- | ----------------------------------------------------- |
| `BETTER_AUTH_SECRET`   | always     | `scripts/ensure-env.js` (generates on first run)      |
| `APP_ORIGIN`           | always     | `scripts/ensure-env.js` (`http://localhost:3001`)     |
| `VITE_JAZZ_APP_ID`     | cloud only | scaffolder (`create-jazz --hosting hosted`) or manual |
| `VITE_JAZZ_SERVER_URL` | cloud only | scaffolder or manual                                  |
| `JAZZ_ADMIN_SECRET`    | cloud only | scaffolder or manual                                  |
| `BACKEND_SECRET`       | cloud only | scaffolder or manual                                  |

In self-hosted development the `jazzPlugin` sets `VITE_JAZZ_SERVER_URL` and
`BACKEND_SECRET` for the local Jazz server it starts. The Effect API needs all
three Jazz values in production.

## Deploying to production

For cloud-hosted deployments, set `BETTER_AUTH_SECRET`, `APP_ORIGIN`, and the
four `VITE_JAZZ_*` / `JAZZ_*` / `BACKEND_SECRET` values in your hosting
provider, then run `pnpm build` and `pnpm start`. The build bundles the server
into `server-dist/index.js`, which serves the client from `dist/` and the API.

For self-hosted deployments you need to run your own Jazz server pointed at the
Hono server's JWKS endpoint, with matching issuer and audience:
`jazz-tools server <APP_ID> --jwks-url https://<your-host>/api/auth/jwks
--jwt-issuer https://<your-host> --jwt-audience https://<your-host>`.

## JWT configuration

`APP_ORIGIN` defaults to `http://localhost:3001`, the Better Auth server.
Set it consistently in `.env` or the process environment. Better Auth and
the Jazz plugin use it for matching issuer/audience settings and the JWKS
endpoint; a frontend proxy does not change the token's issuer or audience.

## Known limitations

- **In-memory user store.** `server/auth.ts` uses an in-memory BetterAuth
  adapter. Restart wipes accounts. Swap for a persistent adapter (Prisma,
  Drizzle, etc.) before shipping.
- **One Better Auth secret per environment.** Rotating the secret invalidates
  every existing JWT.

## Where to go next

- `server/auth.ts` — the place to wire up a persistent BetterAuth adapter.
- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.
- `server/jazz-api.ts` — add Effect routes; `JazzBackend.asAuthority` runs one
  with the backend's own permissions instead of the user's.
- The [Effect guide](https://jazz.tools/docs/recipes/effect) for transactions,
  durability waits and typed errors.

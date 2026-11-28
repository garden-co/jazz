# StagePlan (Effect UI + Hono + Better Auth)

StagePlan's show board in [Effect UI](https://ui.effect.website/), with Jazz
through `jazz-tools/effect-ui`. Read the [family README](../../README.md) for
the data model and permissions. The server is the same as
[`ts-effect-betterauth`](../ts-effect-betterauth)'s; only the browser differs.

Effect UI has no npm release yet: this app installs a snapshot of
`@effect/ui` pinned to one commit, and `jazz-tools/effect-ui` is experimental.

## How Jazz fits Effect UI

The binding adds no reactive primitive of its own. It uses the pieces Effect
UI already has:

- **Live queries are Subscriptions.** A component lists
  `liveQuery("tasks", props.show.id, { query, onRows, onError })` in its
  `subscriptions`. Every result arrives as a message (`GotTasks`), and the
  component keeps the rows in its Model. When the key changes, the old query's
  subscription closes and the new one starts (`src/Board.ts`).
- **Writes are Commands.** `AddTask`, `MoveTask`, `CreateShow` and
  `CreateProfile` are `Command.Service`s. Each one's body is a `Jazz` Effect
  from `src/model.ts`, which the server shares. `Effect.match` turns
  `JazzError | JazzWriteRejected` into a `FailedWrite` message. The local
  write shows up through the live queries, so no Command declares
  `invalidates` or an optimistic patch.
- **Who is signed in is a service.** `JazzSession.layer({ serverUrl: "/jazz" })`
  opens the session at the root (`src/main.ts`). The root follows Better
  Auth's session and `sessionState(...)` (`src/App.ts`). When an account is
  ready, it renders the planner, which `Component.provide` keys on that account
  with `JazzSession.jazz` (`src/Planner.ts`). After a sign-in or sign-out, a
  Command calls `JazzSession.refresh`. A new account then mounts a new
  planner, which gets its own `Jazz`.
- **Rows travel in messages**, so they need Effect Schemas: `jazz.effect.generated.ts`, which `jazz-tools generate --effect-schema` wrote and plain `generate` keeps current.
  Messages are logged, replayed and shown in Effect UI's devtools like any
  others. The errors from `jazz-tools/effect` are Schema classes for the same
  reason.

## Run it

```sh
pnpm dev          # http://127.0.0.1:3000, data in .jazz/
pnpm build        # builds dist/ (needs JAZZ_APP_ID)
pnpm start        # production server on APP_ORIGIN's port (or PORT)
pnpm test:e2e     # Playwright smoke against `pnpm dev`
```

Production sets `APP_ORIGIN`, `JAZZ_APP_ID` and `BETTER_AUTH_SECRET`, and
optionally `JAZZ_SERVER_URL` with `JAZZ_ADMIN_SECRET`.

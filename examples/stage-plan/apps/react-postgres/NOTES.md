# StagePlan on plain Postgres with hand-written sync

What a team builds without a sync product: Postgres, a small Node server, REST reads, RPC
writes, a WebSocket that says "these tables changed", and a client that refetches.

## Versions

- Postgres 18 (Ubuntu apt package), local, same machine. No `wal_level` change needed.
- App server: Hono 4.10 on `@hono/node-server`, `pg` 8.16, `ws` 8.23, zod 4 (request
  validation), run with `tsx`.
- Client: React 19.2.4, Vite 8.0.1, **TanStack Query 5.107** (the cache, dedupe and
  invalidation a team would naturally reach for), same UI packages as the Jazz copy.

## Machine setup (once)

As in `stage-plan/zero/NOTES.md`, minus the `wal_level` step:

```sh
sudo apt-get update && sudo apt-get install -y postgresql
# Only if pg_lsclusters is empty: sudo pg_createcluster 18 main --start
sudo service postgresql restart
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'pass';"
```

Playwright's Chromium on Ubuntu 26.04: see the Zero NOTES; this machine also needed
`libxfixes3`. `harness.mjs` runs `pnpm install --frozen-lockfile` on a fresh checkout and starts
Postgres if `pg_isready` fails.

## Layout

- `db/001_init.sql`: the tables (the same as the Zero port's); `shared/types.ts`: their row types.
- `server/permissions.ts`: the README's "Who can do what": a `canSeeShow` SQL fragment every
  read puts in its WHERE clause, and asserts the writes call inside their transaction.
- `server/reads.ts`: REST GETs (`/api/shows`, `/api/shows/:id/tasks`, ...), filtered by
  permission, so outsiders get `[]` / `null`.
- `server/writes.ts`: RPC writes, `POST /api/rpc/<name>`: one transaction each, with its
  checks; it records which tables changed under which topic.
- `server/live.ts`: the WebSocket hub. Topics are `show:<id>` (subscribing checks crew
  membership) and the implicit `me` (your profile, shows list, checklist).
- `server/db.ts`: the pool and the transaction helper that publishes after `COMMIT`.
- `server/session.ts`, `server/index.ts`: anonymous sessions, routing, static files.
- `src/data/`: the client's data layer: `api.ts` (fetch), `socket.ts` (reconnecting socket,
  refcounted subscriptions), `live.ts` (`useQuery` on TanStack Query, invalidation),
  `queries.ts` (each query's URL and which topic/tables it watches).
- `src/model/actions.ts`: the Jazz action names and signatures, as RPC calls.

## How sync works

- **Delivery: the server's own broadcast after a write commits**, not `LISTEN/NOTIFY`. There is
  one server process, so an in-process broadcast reaches every client; NOTIFY would only be
  needed once there are several server instances (or writers that bypass the server).
- Events carry no row data, only `{ topic, tables }`. The client invalidates the TanStack
  queries that watch those tables under that topic, and they refetch through the same
  permission-checked REST endpoints. So the WebSocket never leaks rows; refetching a 100-task
  board on every change is the price.
- Race handling: a query waits until its topic's subscription is acknowledged before its first
  fetch, so a change can't fall between fetch and subscribe. A query whose topics come from its
  own data (the show list) refetches once when those subscriptions are acknowledged. After a
  reconnect, every query refetches.
- Removing someone from a crew revokes their subscription to the show's topic.
- The writer gets its own change event too: one code path for "show what changed".

## Optimistic updates

Only where a typical team would:
- **Moving a card** on the board (drag and drop / arrow keys): patched into the cache at once,
  reverted by a refetch if the server refuses.
- **Ticking a checklist item.**

Everything else (adding a task, comments, renames, joining, creating a show) waits for the
server's change event. No offline support.

## Auth

Anonymous guests: `GET /api/session` sets an HMAC-signed, HTTP-only `sid` cookie with a random
account id on the first visit, and returns the account and its crew profile (so the bootstrap
needs no second request). The WebSocket upgrade reads the same cookie.

## Behaviour differences and approximations

Same as the Zero port, for the same reasons:
- **Notes merge.** Last write wins (Jazz merges concurrent edits to notes).
- **Offline.** No offline writes; the "Sync" switch only shows whether the live socket is up.
- **Invite check.** The join call checks the code against `showInvites`; `showCrew` has no
  `inviteCode` column.
- **Chief repair / partial demo show.** Writes are atomic, so `ensureChiefSetup` is gone and
  "Save the rest" re-runs the whole demo show. The demo board is filled in by the server.
- **Delete button.** Shown to the chief or the task's creator; the server checks the same.
- **Timestamps.** Explicit `createdAt` / `updatedAt` columns (epoch ms), set by the server.
- Creating a show waits for the server before opening it (the Jazz UI already `await`s it).
- Not ported (unused by the UI): show delete, comment delete.

## Harness

- `seed()` drops and recreates the `stage_plan_postgres` database, applies `db/*.sql` and
  bulk-inserts the fixture as the seed chief (`scripts/seed.ts`, the Zero port's).
- `start()` builds with Vite and serves the app server on `:4320` (built app, API, WebSocket).
  `rttUrl` is `/api/health`.

## Permission checks

`pnpm exec tsx scripts/check-permissions.ts` (after `pnpm seed`, with the server running) runs
the README's rules over HTTP as an outsider, a crew member and the seed chief's data.

## Where the time goes (smoke run, `--runs 1 --samples 3`; canonical runs are separate)

join 424 ms, reload 321, add task 43, move to Done 38, cross-client 55; 236 KB gzip JS.
- A write that isn't optimistic is POST (one transaction, ~6 queries plus a durable commit) →
  change event → GET of the board (100 tasks) → re-render.
- The optimistic move is patched into the cache within ~2 ms; the rest is React re-rendering
  `ShowPage` (including the closed `ShowDialog`'s date picker) and GC, ~30 ms in this headless
  Chromium. The refetch after the server's event renders the board a second time.
- Cross-client is the same path as a non-optimistic write, seen from the other tab.

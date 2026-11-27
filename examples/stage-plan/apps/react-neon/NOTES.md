# StagePlan on Neon Realtime

## Versions

- `@neon/realtime` 0.2.1 (client + server SDK, public beta, launched 2026-10-16), `neon` CLI 9.5.0,
  `@neon/config` 1.14.0 (`neon.ts`)
- Neon Postgres 18, project `jazz-app-comparisons` in **aws-us-east-1** (one of Realtime's four beta
  regions), branch `stage-plan`, Free plan, default compute (0.25–2 CU, scale to zero)
- App server: Hono 4.10 on `@hono/node-server`, run with `tsx`; `pg` 8.16; `jose` for the guest JWT
- React 19.2.4, Vite 8.0.1, zod 4, same UI packages as the Jazz copy

## Setup

A Neon login is needed: **Realtime isn't available on claimable (unclaimed) projects** until they're
claimed (Realtime FAQ), so this can't run on an instant database like InstantDB's temporary apps.
With `NEON_API_KEY` set (or after `neon login`), `harness.mjs` does the rest
through `harness/adapters/neon.mjs`: `pnpm install`, find or create the `jazz-app-comparisons`
project in aws-us-east-1, create the `stage-plan` branch, `neon link`, `neon deploy` (applies
`neon.ts`: Realtime and its allowed browser origins) and `neon env pull` into `.env.local`
(`DATABASE_URL`, `NEON_REALTIME_URL`, `NEON_REALTIME_SECRET`).

Enabling Realtime turns on logical replication for the whole project (permanent `wal_level =
logical`) and restarts its computes; see the Realtime overview.

## Layout (as in Neon's Realtime guides and the `realtime-react` template)

- `db/001_init.sql`: the Postgres schema (same tables as the Zero port); `schema.ts`: row types
- `server/queries.ts`: the live queries, raw SQL per query name (`rawSql`). The server fills in the
  signed-in account and seals each one (`realtime.seal`); the browser gets an opaque capability it
  can subscribe with but can't read or change
- `server/mutators.ts`: the write handlers: validate (zod), authorize, write in one transaction,
  return the transaction id (`pg_current_xact_id()` in the writes' `RETURNING`)
- `server/index.ts`: Hono: guest sign-in, `POST /api/live` (seal a named query), `POST
  /api/mutate/:name`, `/api/health`, and the built app
- `queries.ts`: the query names and their row types, for the browser
- `src/model/live.ts`: the Realtime client, `useQuery` and `mutate` (below);
  `src/model/actions.ts` keeps the Jazz action names and signatures
- `src/`: the Jazz UI, with the Zero port's `useQuery`/`useZero` imports swapped for these

## Permissions

The README's "Who can do what", enforced on the server, the way Neon's docs recommend:

- **Reads:** Realtime authorizes the *query*, not the connection. Each query joins through
  `showCrew` (or the show's chief, the invite's chief, the checklist's owner) for the account from
  the session cookie, and is sealed so it can't be changed. Because the membership check is part of
  the live query, it stays live: a member who leaves stops seeing the show's rows.
- **Writes:** checks in the handlers, the same as the Zero port's server-side mutator checks.
- **No RLS.** Realtime doesn't support row-level security yet ("aim to support it by GA"); a live
  query on a table with RLS is rejected, and the docs recommend filtering in the query instead.
- `pnpm exec tsx --env-file=.env.local scripts/check-permissions.ts` (after `pnpm seed`) runs the
  handlers and the query SQL as different users: 34 checks, all passing. It writes test rows, so seed
  again before a benchmark run.

## Auth

Anonymous guests, the same as the Zero port: `GET /api/login` issues an HS256 JWT in an HTTP-only
cookie whose `sub` is a fresh account id, and the sealing and write routes read it. Neon's documented
auth is Managed Better Auth (used by BandChat and Wequencer), but it has **no anonymous plugin**
(its supported plugins: email/password, OAuth, email OTP, magic link, phone, admin, organization,
JWT). The docs' auth model is "use your normal auth context in the request handler that seals the
query", so a server-issued guest session is within it. A guest's crew profile id is their account id.

## Reads, writes and optimistic updates

- **`useQuery` instead of `useLiveQuery`.** `@neon/realtime-react`'s `useLiveQuery(sealedQuery)`
  needs the sealed query before it can subscribe, so the docs fetch it in a parent component and
  render the child once it has it. The Jazz components call their queries inline (`useAll(...)`),
  and the port keeps them unchanged, so `src/model/live.ts` has a `useQuery(queries.x.y(args))`
  hook that fetches the sealed query and subscribes the way the binding does: a materialized
  `client.subscribe()` plus `QueryRefreshController` (renews 10 s before the 60 s expiry), read
  with `useSyncExternalStore`. The reactivity guide describes the binding as exactly that layer.
  Every query is one HTTP round trip to the app server (seal), then a subscription over the one
  shared WebSocket.
- **Optimistic writes** follow the mutations guide's "without a framework" pattern: each write puts
  an overlay on the rows of the live queries it touches (a reducer per query name), posts to the
  server, waits with `awaitTxId(txid, 10_000)` until those subscriptions have synced its
  transaction, then drops the overlay, so nothing flickers. A rejected write drops it at once and
  shows the toast. Task add/move/assign/rename/notes/delete, comments, checklist, crew rename,
  invite rotation, show edit and crew removal are optimistic; show creation and joining aren't
  (the UI waits for them anyway).
- The docs' other two options: React's `useOptimistic` (per component; here the board's writes
  come from dialogs too) or TanStack DB collections (`@neon/realtime-tanstack`, a normalized
  client store). The overlay keeps the components unchanged.
- **Ids and order.** Clients pick ids (`crypto.randomUUID()`), so the synced row replaces the
  optimistic one. Every `ORDER BY` ends in the id, so the order is deterministic.

## Behaviour differences and approximations

As in the Zero port (and the same in every port):

- **Notes merge.** Postgres has no text CRDT: the last write wins (Jazz merges).
- **Offline.** Realtime doesn't take writes while disconnected and can't pause syncing. The top
  nav's "Sync" switch only shows whether any live query has gone stale or failed (disabled).
- **Invite check.** The join handler checks the code against `showInvites`; no `inviteCode` on the
  membership row.
- **Chief repair / partial demo show.** Each write is one transaction, so `ensureChiefSetup` is gone
  and "Save the rest" re-runs the whole demo show.
- **New-account check.** The server's `crew.ensure` says whether the profile is new (an
  `INSERT … ON CONFLICT DO NOTHING`), so a returning account never gets a second demo show.
- **"My shows" task counts** come from a correlated `jsonb_agg` subquery in the live query (Zero:
  `.related("tasks")`).
- Not ported (unused by the UI): show delete, comment delete.

## Harness

- `seed()` drops and recreates StagePlan's tables on the Neon branch (the database stays: Realtime
  is enabled on the branch) and bulk-inserts the fixture as the seed chief; returns `{ showId,
  code }` of show-0 (also saved to `.bench-ctx.json` for `--no-seed`).
- `start()` builds with Vite (`VITE_NEON_REALTIME_URL`), then serves the app server on `:4320`
  (built app + routes). `http://127.0.0.1:4320` is in `neon.ts`'s Realtime `allowedOrigins`:
  Realtime rejects WebSockets from other origins.
- **rtt** is `GET /api/health`: a `SELECT 1` on Neon through the local app server (about 4 ms; a
  bare `SELECT 1` from Node is about 1.4 ms, so this sandbox runs in us-east-1 too). The Realtime
  endpoint itself takes 80–250 ms to answer a plain HTTP GET, which isn't network distance, so it
  isn't used.

## Smoke run (2026-11-14, `--runs 1 --samples 3`, not a canonical result)

join 634 ms, reload 499 ms, add task 15 ms, move to Done 17 ms, cross-client 93 ms; 252 KB gzip
loaded; rtt 4.3 ms; 0 lost samples.

## Beta limitations seen

- Claimable projects can't use Realtime; regions are limited to four (us-east-1/2, eu-central-1,
  ap-southeast-1).
- No RLS (above). The transport protocol isn't stable yet and the docs make no performance
  guarantees for the beta.
- Every subscription needs a sealed query from your server, and a renewal every ~50 s per live
  query.

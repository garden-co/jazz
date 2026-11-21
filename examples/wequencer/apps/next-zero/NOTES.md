# Wequencer on Rocicorp Zero

## Versions

- `@rocicorp/zero` 1.12.0, `zero-cache` from the same package (`NODE_ENV=production`)
- Postgres 18.6 (apt), `wal_level = logical`, local
- Next.js 16.2.11 (same as the Jazz copy), Better Auth 1.7.22 on Postgres through a `pg.Pool`, its
  tables in the `auth` schema (Better Auth's own `getMigrations().runMigrations()`), not synced
- React 19.2.4, zod 4

Machine setup (Postgres via apt, `wal_level`, the `postgres` password, Playwright on Ubuntu 26.04):
see `../../../stage-plan/apps/react-zero/NOTES.md`, "Machine setup". `harness.mjs` does the rest, starting
from `pnpm install`.

## Layout

- `db/001_init.sql` + `schema.ts`: Postgres and Zero schemas (the Jazz column names are kept)
- `queries.ts`: named queries; read permissions are their filters (resolved server-side)
- `mutators.ts`: custom mutators; write permissions are server-side checks in them
- `app/api/zero/{query,mutate}/route.ts`: Zero's endpoints as Next route handlers (as in Zero's
  Next.js docs); `app/api/auth/[...all]` is Better Auth, unchanged; `app/api/bootstrap` upserts the
  profile with SQL
- `components/zero-provider.tsx` replaces `jazz-provider.tsx`; the root layout reads the Better
  Auth session on the server and passes the user in, so Zero starts without an extra round trip
- `lib/session-setup.ts` keeps the Jazz helpers' names (`createSession`, `addTrack`, `stepRow`, …)
  and calls the mutators; `lib/report-write.ts` turns a mutation's `.server` result into the
  promise `reportWrite` expects

## Auth

Email + password through Better Auth. zero-cache forwards the session cookie to the route
handlers (`ZERO_*_FORWARD_COOKIES=true`), which call `auth.api.getSession`. zero-cache runs on
the page's host (`127.0.0.1:4868`). The account id shown on the dashboard (and pasted into the
members dialog) is the Better Auth user id.

## Behaviour differences and approximations

- **Creator authority**: Jazz's immutable `$createdBy` is a `created_by` column, set only by the
  `sessions.create` mutator (no mutator updates it).
- **Pads**: still sparse rows with the derived id, upserted (`steps.set`). The server also refuses
  to rewrite a squatted id that sits in a session the writer can't edit.
- **Concurrent edits**: Zero applies mutations in server order, so the last write to a pad wins.
  That's the same outcome as Jazz's row merge for a single boolean.
- **Offline**: Zero doesn't take writes while disconnected (the landing page copy "in real time and
  offline" is unchanged from the Jazz UI).
- **Audio and presence** are kept as in the Jazz copy (the harness doesn't touch them). Presence
  is a `presence.beat` upsert every 5 s.
- Timestamps (`observed_at`, `heartbeat_at`) are epoch ms. `lib/transport.ts` (UI) still builds
  `Date`s, which `observe()` converts.

## Seed

`pnpm seed` (or `seed()`) resets `wequencer_zero`, runs the migrations, signs the three fixture
users up through Better Auth's server API, creates their profiles with the app's `ensureProfile`,
then bulk-inserts 100 sessions (3 members, 1 pattern, 16 tracks, the fixture's pads and one
transport observation each) straight into Postgres. Track names, colours and instruments, and the
pad ids, come from the app's own helpers, as in the Jazz seed. Returns
`{ sessionId, onPads, users }`.

## Permission checks

`BETTER_AUTH_SECRET=… pnpm exec tsx scripts/check-permissions.ts` (after `pnpm seed`) runs the server mutators and
queries as different users against the seeded database (outsider, editor, viewer, creator). It
writes test rows, so seed again before a benchmark run. The StagePlan and BandChat ports have the
same script.

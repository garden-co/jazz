# BandChat on Rocicorp Zero

## Versions

- `@rocicorp/zero` 1.12.0, `zero-cache` from the same package (`NODE_ENV=production`)
- Postgres 18.6 (apt), `wal_level = logical`, local
- Better Auth 1.7.22 (same as the Jazz copy) on Postgres through a `pg.Pool`, its tables in the
  `auth` schema (created by Better Auth's own `getMigrations().runMigrations()`), which Zero
  doesn't replicate
- Hono 4.10 on `@hono/node-server` (run with `tsx`), React 19.2.4, Vite 8.0.1, zod 4

Machine setup (Postgres via apt, `wal_level`, the `postgres` password, Playwright on Ubuntu 26.04):
see `../../../stage-plan/apps/react-zero/NOTES.md`, "Machine setup". `harness.mjs` does the rest, starting
from `pnpm install`.

## Layout

- `db/001_init.sql` + `schema.ts`: Postgres and Zero schemas
- `queries.ts`: named queries; read permissions are their filters (resolved server-side)
- `mutators.ts`: custom mutators; write permissions are server-side checks in them
- `server/`: Better Auth (`/api/auth/*`), Zero's `/api/zero/query` and `/api/zero/mutate`, the SPA
- `components/zero-provider.tsx` replaces `jazz-provider.tsx`; the components swap `useAll` and
  `db.*` for `useQuery` and `zero.mutate`

## Auth

Email + password through Better Auth, as in the Jazz copy. The session cookie is forwarded by
zero-cache (`ZERO_*_FORWARD_COOKIES=true`) to the endpoints, which call
`auth.api.getSession`. zero-cache runs on the page's host (`127.0.0.1:4858`) so the browser sends
it the cookie. Zero's `userID` is the Better Auth user id (the Jazz app's `author`).

## Behaviour differences and approximations

- **Attachments and avatar images are left out** (SPEC: out of a first round's reach). Zero has
  no binary column type to sync. The Attach button and profile photo picker are still rendered;
  picking a file shows an error, and photos aren't saved. `Attachment.tsx` is gone.
- **Sketches are ported**: strokes' points are a `jsonb` array.
- **Offline**: Zero doesn't take writes while disconnected (the sign-in copy still says
  "saved on your device first", unchanged from the Jazz UI).
- **Unsent messages**: tracked through each mutation's `.server` result instead of Jazz's
  transaction-id matching and `onMutationError`.
- **Rejoin** for a creator without a membership uses the same `members.admit` mutator (the
  creator admitting themselves).
- `$createdAt`/`$createdBy` are explicit `createdAt`/`createdBy` columns (epoch ms).

## Seed

`pnpm seed` (or `seed()`) resets `band_chat_zero`, runs the migrations, signs the three fixture
users up through Better Auth's server API (`auth.api.signUpEmail`), then bulk-inserts profiles,
256 rooms, the 768 memberships and the 10,000 messages straight into Postgres. Differences from
the Jazz seed:

- Memberships are written directly, not through the app's ask-to-join/admit flow.
- Messages keep the fixture's `createdAtMs` (Jazz's get the seed run's insert time).
- `lastActivityAt` is the room's creation time and there are no read markers, as in the Jazz
  seed: every room reads unread, so the room list opens one unread-count query per room
  (256), as the Jazz UI does.

Returns `{ busyRoomId, quietRoomId, users }`; `signIn` posts to Better Auth's sign-in endpoint.

## Observations

- `bc_send` is ~350–450 ms locally. A CPU profile shows the time going to React re-rendering the
  copied UI (astryx `Timestamp` formatting, tooltips, layers for 200 bubbles and 256 room rows),
  not to Zero: turning the 256 unread queries off, or the client-side `lastActivityAt` bump, did
  not change it.
- The client logs "Slow query materialization" warnings while the 256 unread-count queries hydrate.
- zero-cache's startup warnings are the same as StagePlan's (see its NOTES).

## Permission checks

`BETTER_AUTH_SECRET=… pnpm exec tsx scripts/check-permissions.ts` (after `pnpm seed`) runs the server mutators and
queries as different users against the seeded database and checks the README's "Who can do what"
(outsiders, members, the creator or chief). It writes test rows, so seed again before a benchmark
run.

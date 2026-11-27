# BandChat on Neon Realtime

## Versions

- `@neon/realtime` 0.2.1 (public beta), `@neon/auth` 0.8.2 (Managed Better Auth client), `neon` CLI
  9.5.0, `@neon/config` 1.14.0
- Neon Postgres 18, project `jazz-app-comparisons` in **aws-us-east-1**, branch `band-chat`, with
  Realtime and Managed Better Auth enabled through `neon.ts` (`auth: true`)
- Hono 4.10 on `@hono/node-server` (run with `tsx`), `pg` 8.16, `jose` (JWT verification), React
  19.2.4, Vite 8.0.1, zod 4

Setup and the shared harness adapter: see `../../../stage-plan/apps/react-neon/NOTES.md` (a Neon API key is
needed; Realtime isn't available on claimable projects). For BandChat the adapter also pulls the
auth env and adds `http://127.0.0.1:4321` to Managed Better Auth's trusted domains (`neon neon-auth
domain add`): sign-ins from origins that aren't trusted fail with `INVALID_ORIGIN`, and only
`localhost` is trusted out of the box.

## Layout (as in Neon's Realtime guides chat app: Vite + Hono + Managed Better Auth)

Started from the Zero port's copy of the Jazz UI; only the data layer differs.

- `db/001_init.sql` (the Zero port's tables) + `schema.ts` (row types)
- `server/queries.ts`: the live queries, raw SQL per query name; the server fills in the signed-in
  user and seals each one
- `server/mutators.ts`: write handlers: validate, authorize, write in one transaction, return the
  txid (`pg_current_xact_id()` in the writes' `RETURNING`)
- `server/index.ts`: `POST /api/live` (seal), `POST /api/mutate/:name`, `/health`, the SPA
- `queries.ts`, `mutators.ts`: what the browser calls (query names and args; each write's name, args
  and optimistic overlay). The components keep the Zero port's `useQuery(queries.x.y(args))` and
  `db.mutate(mutators.x.y(args))` calls
- `src/lib/live.ts`: the Realtime client, `useQuery`, and `mutate` with the optimistic overlay (the
  same as StagePlan's; see its NOTES for why it isn't `useLiveQuery`)

## Auth

Email + password through **Managed Better Auth**, the way Neon's Realtime guides do it for Vite +
Hono: the browser's `@neon/auth` client signs in against the branch's auth URL directly and sends
the session's JWT as a bearer token. The server verifies it against the branch's JWKS
(`jose.jwtVerify`, issuer and audience = the auth origin), and seals queries for its `sub`. The
Realtime connection carries no user credential: the sealed query is the authorization. The JWT
lasts about 15 minutes; `getSession()` refreshes it.

## Permissions

The README's rules, server-side: read rules are the filters of the sealed queries (membership
joins, so they stay live: a removed member's subscriptions stop matching rows); write rules are
checks in the handlers, the same as the Zero port's server-side mutator checks. No RLS (Realtime
rejects tables with RLS). `pnpm exec tsx --env-file=.env.local scripts/check-permissions.ts` (after
`pnpm seed`): 28 checks, all passing.

## Behaviour differences and approximations

As in the Zero port:

- **Attachments and avatar images are left out** (SPEC: out of a first round's reach). The Attach
  button and profile photo picker are still rendered; picking a file shows an error.
- **Sketches are ported**: strokes' points are a `jsonb` array.
- **Offline**: Realtime doesn't take writes while disconnected (the sign-in copy still says
  "saved on your device first", unchanged from the Jazz UI).
- **Unsent messages**: tracked through each write's server result.
- `$createdAt`/`$createdBy` are explicit `createdAt`/`createdBy` columns (epoch ms, the server's
  clock).

Neon-specific:

- **Optimistic writes** for every write (messages, reactions, read markers, sketches, rooms,
  members, join requests, profiles): an overlay on the live queries the write touches, dropped once
  `awaitTxId` sees the write's transaction on those subscriptions (the mutations guide's "without a
  framework" pattern).
- **Unread counts** keep the Jazz UI's one query per room: about 260 live queries at startup, each
  sealed by one `POST /api/live`, then subscribed over the one shared WebSocket. They all subscribed
  fine in the smoke runs: no Realtime errors or timeouts.

## Seed

`pnpm seed` (or `seed()`) drops and recreates BandChat's tables on the branch, signs the three
fixture users up through Managed Better Auth's HTTP API (`/sign-up/email`, or `/sign-in/email` when
an earlier seed already created them: the auth users persist on the branch), then bulk-inserts
profiles, 256 rooms, the 768 memberships and the 10,000 messages straight into Postgres. As in the
Zero seed: memberships are written directly (not through ask-to-join/admit), messages keep the
fixture's `createdAtMs`, and there are no read markers, so every room reads unread.

Returns `{ busyRoomId, quietRoomId, users }`; `signIn` fills in the app's own sign-in form, which
reloads the page once signed in (the harness reports that as a self-reload during the sign-in, as
for InstantDB).

## Harness

`start()` builds with Vite (`VITE_NEON_REALTIME_URL`, `VITE_NEON_AUTH_URL`) and serves the app
server on `:4321`. rtt is `/health`: a `SELECT 1` on Neon through the app server (see StagePlan's
NOTES).

## Smoke run (2026-11-14, `--runs 1 --samples 3`, not a canonical result)

open 1328 ms, resume 1627, send 455, switch room 403, cross-client 1357; 305 KB gzip loaded; rtt
3.9 ms; 0 lost samples. The Zero port on the same machine, same settings: open 1018, resume 1900,
send 294, switch 332, cross-client 864 (this sandbox runs the shared UI about 1.2× slower than the
canonical machine).

**Where `bc_send` goes** (in-page measurements): the optimistic bubble is in the first render, but
the task that commits it also runs a second full render of the room before the browser paints: the
room view's read-marker write, which the new activity triggers, applies its own overlay
synchronously. Each render of the copied UI (200 bubbles, astryx `Timestamp` formatting) costs about
230 ms here. A send then causes about 11 renders of the room in all: each write's overlay goes on,
its transaction syncs through two or three live queries (messages, rooms, read markers), each
arriving as its own update, and the overlay comes off once `awaitTxId` resolves. That is the docs'
optimistic pattern; no batching beyond it was added.

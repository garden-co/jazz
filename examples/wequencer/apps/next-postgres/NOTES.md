# Wequencer on plain Postgres with hand-written sync

The same sync design as `stage-plan/postgres` and `band-chat/postgres`: REST reads, RPC writes,
a WebSocket that says which tables changed, and TanStack Query refetching what an event names.
This one is a Next.js app, which changes how the pieces are hosted.

## Versions

- Postgres 18 (apt), local. Next.js 16.2.11 (as the Jazz copy), Better Auth 1.7.22 on Postgres
  (`auth` schema, its own migrations).
- Hono 4.10 mounted in a Next route handler (`hono/vercel`), `pg` 8.16, `ws` 8.23, zod 4.
- React 19.2.4, TanStack Query 5.107.

Machine setup: `../../../stage-plan/apps/react-postgres/NOTES.md`. `harness.mjs` does the rest, starting from
`pnpm install`.

## Layout

- `db/001_init.sql` (the Zero port's tables) + `schema.ts` (row types): schema.
- `app/api/[[...route]]/route.ts` → `lib/api.server.ts`: the Hono app with the REST reads and
  RPC writes (`POST /api/rpc/<name>`). Better Auth and the profile bootstrap keep their routes.
- `lib/permissions.server.ts`: SQL fragments and asserts for the README's rules.
- `lib/db.server.ts`: pool and the transaction helper, which `pg_notify`s what changed.
- `live-server.ts` + `lib/live.server.ts`: the WebSocket server, a separate small process.
- `lib/data/` (`api`, `socket`, `live`), `queries.ts`, `mutators.ts`,
  `components/data-provider.tsx`: the client data layer. `lib/session-setup.ts` keeps the Jazz
  helper names.

## How sync works here

- **Delivery: `LISTEN/NOTIFY`**, unlike StagePlan and BandChat (in-process broadcast).
  - Next's route handlers can't hold WebSockets, so the socket server is its own process.
  - A custom Next server (`next()` + `http.createServer`) didn't work: Next attaches its own
    `upgrade` listener on the first request and hangs up on unknown upgrade paths.
  - So: every write's transaction `pg_notify`s `{ changes, revoke }`, which Postgres delivers
    only on commit. `live-server.ts` `LISTEN`s and broadcasts to subscribers.
  - The browser connects to `ws://<host>:<port + 1>/`, with Better Auth's cookie (cookies
    aren't port-scoped).
- Topics: `session:<id>` (members and the creator) and `me` (your session list and profile).
  Adding a member tells their `me`; removing one revokes their subscription.
- **One steps request per pattern.** The 16 lanes share one cached query
  (`/api/sessions/:id/patterns/:pid/steps`) and each `select`s its own track. TanStack's
  structural sharing keeps the other lanes from re-rendering when one pad changes.
  `TrackLane` passes `track.session_id` to the query (for the topic).

## Optimistic updates

Only **pad toggles**: the pad flips in the cache at once, as TanStack's docs do it (cancel
in-flight refetches, then patch). If the server refuses, the pattern refetches. Everything
else (tracks, members, transport, presence) waits for the server's change event.

## Behaviour differences and approximations

As in the Zero port:
- Creator authority is a `created_by` column.
- Pads are sparse rows with the derived id, upserted, so concurrent first presses write the
  same row. The last write to a pad wins (Jazz merges a single boolean the same way).
- Presence is a `presence.beat` upsert every 5 s, which re-fetches presence for every member.
- No offline writes.
- Timestamps are epoch ms.
- `createSession` resolves once the server has the session (Zero waited for the local apply).

## Seed

The Zero port's script: `wequencer_postgres` is reset, the three users sign up through Better
Auth, profiles come from the app's `ensureProfile`, and the 100 sessions are bulk-inserted.

## Observations (smoke run, `--runs 1 --samples 3`)

open 559 ms, reload 379, toggle 8, cross-client 41, concurrent 708; 327 KB gzip JS.
- A toggle is a cache patch plus the lane's re-render.
- Cross-client is POST → NOTIFY → socket → GET of the pattern's steps (~250 rows) → the
  changed lane re-renders.
- Concurrent: 64 toggles from two clients each trigger a NOTIFY and a refetch on both sides.
  TanStack cancels superseded refetches, and both grids converge after the last one.

## Permission checks

`BETTER_AUTH_SECRET=… NEXT_PUBLIC_APP_ORIGIN=… pnpm exec tsx scripts/check-permissions.ts`
(after `pnpm seed`, with `pnpm start` running on :4331/:4332) checks the rules over HTTP as the
creator, an editor and an outsider who is added as a viewer, then removed. 16 checks.

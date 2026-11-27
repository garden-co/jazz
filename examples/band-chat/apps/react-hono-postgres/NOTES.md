# BandChat on plain Postgres with hand-written sync

Same architecture as `stage-plan/postgres` (see its NOTES for the sync design): Postgres, a
Hono + `pg` + `ws` server with REST reads and RPC writes, a WebSocket that says which tables
changed, and TanStack Query on the client refetching what an event names.

## Versions

- Postgres 18 (apt), local. Better Auth 1.7.22 (as the Jazz copy) on Postgres through a
  `pg.Pool`, its tables in the `auth` schema (created by Better Auth's own migrations).
- Hono 4.10 on `@hono/node-server`, `pg` 8.16, `ws` 8.23, zod 4, run with `tsx`.
- React 19.2.4, Vite 8.0.1, TanStack Query 5.107.

Machine setup: `../../../stage-plan/apps/react-postgres/NOTES.md`. `harness.mjs` does the rest, starting from
`pnpm install`.

## Layout

- `db/001_init.sql` (the Zero port's tables) + `shared/types.ts`: schema.
- `server/permissions.ts`: SQL fragments (`isMemberSql`, `canReadRoomSql`) for the reads, and
  asserts for the writes.
- `server/reads.ts`, `server/writes.ts`, `server/live.ts`, `server/db.ts`: as in StagePlan.
- `src/data/`: `api.ts`, `socket.ts` (batched subscriptions), `live.ts` (`useQuery`),
  `queries.ts`, `mutators.ts` (`db.mutate(mutators.x.y(args))` → RPC, returning `{ server }`).
- `components/data-provider.tsx` replaces `jazz-provider.tsx`.

## Topics and fan-out

- `room:<id>`: the open room's messages, reactions, members, join requests and strokes.
  Subscribing requires membership or being the creator. Removing a member revokes it.
- `me`: everything in your room list: rooms, memberships, read markers, unread counts,
  profiles you know, your join requests. The server fans out to each affected person's `me`:
  a message goes to `room:<id>` and to every member's `me` (`rooms`, `unread`).
- The client batches subscription changes per tick, so a page subscribes in one message.

## Optimistic updates

Only **sending a message**: the bubble is put into the room's cached newest page at once, and
dropped again (refetch) if the server refuses it; the unsent notice works as in the Zero port.
Everything else waits for the server's change event.

## Behaviour differences and approximations

- **Unread counts are one query** (`GET /api/unread`, capped at 100 per room) for the whole room
  list, read once in `RoomNav` and passed to each row. The Jazz and Zero apps run one query per
  room (256). No team writing REST would send 256 requests per page load; Convex's port did the
  same aggregation. Small UI change in `RoomNav.tsx`.
- **Attachments and avatar images are left out**, as in the Zero port (same UI handling).
  Sketches are ported (strokes' points are `jsonb`).
- **Offline**: no offline writes. The sign-in copy is unchanged from the Jazz UI.
- `SketchCanvas` passes its `roomId` to the strokes query (for the topic).
- `$createdAt`/`$createdBy` are explicit `createdAt`/`createdBy` columns (epoch ms).

## Seed

As the Zero port: `scripts/seed.ts` resets `band_chat_postgres`, signs the three fixture users
up through `auth.api.signUpEmail`, then bulk-inserts profiles, 256 rooms, the memberships and
the 10,000 messages straight into Postgres (not through the ask-to-join/admit flow).

## Observations (smoke run, `--runs 1 --samples 3`)

Two runs: open 1225–1657 ms, resume 1273–1314, send 358–393, switch 459–538, cross-client
467–591; 235 KB gzip JS. Noisy: canonical runs are separate.
- The time is the shared UI's render, as the REPORT found for every stack. Measured alone, the
  server answers a send in ~7 ms, the newest page in ~4 ms and the unread counts in ~13 ms.
  A send's optimistic render of 200 bubbles and 256 room rows takes ~300 ms on this 2-core
  sandbox.
- Refetch-on-notify renders the tree again for each refetch after a send: the room's
  messages, the room list and unread counts, then the read marker.
- With 3 samples, `bc_switch_room`'s median lands on a switch into the busy room (200
  bubbles). Switching to the quiet room is fast. With 20 samples the median is comparable to
  the other stacks'.

## Permission checks

`BETTER_AUTH_SECRET=… pnpm exec tsx scripts/check-permissions.ts` (after `pnpm seed`, with the
server running on :4321) checks the README's rules over HTTP as the creator, a member and a new
outsider (ask to join, admit, removal). 22 checks.

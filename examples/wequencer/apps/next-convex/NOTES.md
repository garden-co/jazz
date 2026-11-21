# Wequencer on Convex: notes

## Versions

- `convex` 1.50.0, local backend `precompiled-2026-10-22-1447815`
- `@convex-dev/auth` 0.0.98 with `@auth/core` 0.37.4, **Password** provider, through
  Convex Auth's Next.js integration: `ConvexAuthNextjsServerProvider` and
  `ConvexAuthNextjsProvider`, with `convexAuthNextjsMiddleware` in `proxy.ts` (Next 16's
  name for middleware.ts). Why not 2.0.0: see `stage-plan/convex/NOTES.md`.
- Next.js 16.2.11 (Turbopack), React 19.2.4, same Astryx / design packages as the Jazz copy

## Backend and hooks

A local anonymous deployment (`CONVEX_AGENT_MODE=anonymous npx convex dev`, no account),
with data in `.convex/`. `harness.mjs` works from a fresh checkout: it runs `pnpm install`
if needed, configures the deployment and auth keys, starts `convex dev`, seeds, runs
`next build` and serves the app with `next start` on :4404. `rttUrl` is the backend's
`/version`.

- **Variants**:
  - `default`: pad toggles wait for the server.
  - `optimistic`: built with `NEXT_PUBLIC_CONVEX_OPTIMISTIC_PADS=1`, so pad toggles use
    `withOptimisticUpdate` on the lane query (see below).
- **`signIn(page, key)`** posts `auth:signIn` to Convex Auth's Next.js route
  (`/api/auth`), which sets the session cookies. The Jazz hook calls Better Auth's API
  the same way. It's untimed.
- **Seed** (`scripts/seed.mjs`, `pnpm seed` with `pnpm backend` running): it mirrors
  `wequencer/jazz/seed.ts`.
  1. It wipes every table.
  2. The seed user, a and b sign up through the Password provider. Their profiles are
     created by the auth callback, the app's own bootstrap.
  3. The internal `seed:createSession` writes each of the 100 fixture sessions in one
     transaction: the session (creator = seed user), its members (seed owner, a and b
     editors), "Pattern 1" of 64 steps, 16 tracks named and coloured as in the Jazz seed,
     the fixture's pads, and a stopped transport observation.

## How the port maps

- **Permissions**: the Jazz `permissions.ts` rules are checks in every function
  (`convex/lib/access.ts`).
  - Members (and the creator) read the session.
  - Owners and editors change tracks, patterns, pads and the transport; viewers only read.
  - Only the creator adds or removes members and removes tracks.
  - A step's track and pattern, and the transport's pattern, must belong to the session.
  - Presence is written only by members, as their own profile.

  I checked these with a script against the live backend (viewer writes, outsider reads,
  cross-session patterns, editor admin attempts).
- **Pads** are sparse rows as in Jazz. Jazz derives a row id from a hash of track,
  pattern and position and upserts it, so two bandmates pressing the same untouched pad
  write one row. Here `steps.set` looks the row up by the
  `(track, pattern, position)` index and patches or inserts it in the same transaction.
  Convex's serializable transactions give the same convergence (I checked that two sets
  leave one row), and `lib/step-id.ts` and `@noble/hashes` aren't needed.
- **Subscriptions**: as in Jazz, each track lane holds its own `steps.lane` query, so a
  pad edit only re-renders its lane. The session page also subscribes to the session,
  members, tracks, patterns, the latest transport observation and presence.
- **Profile bootstrap**: Jazz posts to `/api/bootstrap` after sign-in. Here Convex Auth's
  `afterUserCreatedOrUpdated` callback creates the profile at sign-up, on the server.
  Queries never create profiles.
- **Account IDs** are the Convex Auth user id, which the creator pastes into the members
  dialog.
- **Ids and timestamps**: `_id` replaces `id`, `sessions.creator` replaces `$createdBy`,
  and `observed_at` / `heartbeat_at` are ms numbers. The shared `lib/transport.ts` still
  builds `Date`s, which the two transport writers convert.
- **Redirects**: the Jazz app redirects in its server components (`/` → `/dashboard` when
  signed in, `/dashboard` → `/` when not). Here `proxy.ts` does the same with Convex
  Auth's route matchers, as its Next.js docs show.

## Optimistic updates

- **Default: none.** Convex's optimistic-update docs show a counter increment and a chat
  send. A pad toggle isn't either, so the default port waits for the server.
  `wq_toggle_pad` therefore measures a mutation plus the query update, where Jazz
  measures a local write.
- **`optimistic` variant**: the counter-style update applied to pad toggles
  (`useSetStep` in `components/session-page.tsx`), so both readings are available.

## Approximations and notes

- **Harness self-reloads**: the harness counts a same-document `history.replaceState`
  from the Next App Router after hydration as a "self-reload". It isn't a reload: no
  navigation request is made, and page state survives.
- **Profile names** are readable through presence, as in Jazz: `presence.bySession`
  joins each row's profile for session members.
- **Audio, presence heartbeats, the playhead and the mix** are the copied code.
  Transport and presence rows go through mutations.

# Wequencer on Neon Realtime

## Versions

- `@neon/realtime` 0.2.1 (public beta), `@neon/auth` 0.8.2 (`@neon/auth/next` client and
  `@neon/auth/next/server`), `neon` CLI 9.5.0, `@neon/config` 1.14.0
- Neon Postgres 18, project `jazz-app-comparisons` in **aws-us-east-1**, branch `wequencer`, with
  Realtime and Managed Better Auth enabled through `neon.ts`
- Next.js 16.2.11 (same as the Jazz copy), `pg` 8.16, React 19.2.4, zod 4

Setup and the shared harness adapter: see `../../../stage-plan/apps/react-neon/NOTES.md` (a Neon API key is
needed; Realtime isn't available on claimable projects). The adapter also adds
`http://127.0.0.1:4332` to Managed Better Auth's trusted domains.

## Layout (as in Neon's Next.js Realtime example)

Started from the Zero port's copy of the Jazz UI; only the data layer differs.

- `db/001_init.sql` (the Zero port's tables) + `schema.ts` (row types)
- `lib/live-queries.server.ts`: the live queries, raw SQL per name, built for the signed-in user
- `app/api/live/route.ts`: seals a named query (a Route Handler: sealing and renewal are reads,
  and Next runs Server Actions one at a time, as the example explains)
- `app/actions.ts`: the write **Server Action**, as the example does writes; it checks the session
  itself and returns a typed result (`committed` + txid, `unchanged` or `rejected`) because Next hides
  thrown messages in production. The handlers are in `lib/mutators.server.ts`
- `app/api/auth/[...path]`: Managed Better Auth's API on the app's origin (`createNeonAuth().handler()`)
- `app/api/bootstrap`: the profile upsert, as in the Zero port; `app/api/health`: the harness's rtt
- `queries.ts`, `mutators.ts`: what the browser calls; the components keep the Zero port's
  `useQuery(queries.x.y(args))` and `db.mutate(mutators.x.y(args))` calls
- `lib/live.ts`: the Realtime client, `useQuery`, `mutate` with the optimistic overlay (the same as
  the other heroes'; see StagePlan's NOTES for why it isn't `useLiveQuery`)

## Auth

Email + password through **Managed Better Auth**, mounted on the app's origin the way Neon's
Next.js example does it: `@neon/auth/next`'s client talks to `/api/auth/*`, and server code (the
layouts, the sealing route, the Server Action, the bootstrap) reads the session from its cookie with
`getSession()`. `NEON_AUTH_COOKIE_SECRET` signs the session-data cookie. The browser holds no token
and the Realtime connection carries no user credential: the sealed query is the authorization.

## Permissions

The README's rules, server-side: read rules are the filters of the sealed queries (membership
joins, so they stay live); write rules are checks in the handlers, the same as the Zero port's
server-side mutator checks (a pad's track and pattern, and the transport's pattern, must belong to
the session; viewers only read; the creator administers members and deletes tracks). No RLS.
`pnpm exec tsx --conditions react-server --env-file=.env.local scripts/check-permissions.ts` (after
`pnpm seed`): 20 checks, all passing.

## Behaviour differences and approximations

As in the Zero port:

- **Creator authority**: Jazz's immutable `$createdBy` is a `created_by` column, set only by
  `sessions.create`.
- **Pads**: sparse rows with the derived id, upserted (`INSERT … ON CONFLICT (id) DO UPDATE`); a
  pad id that already exists for another track, pattern or session is refused.
- **Concurrent edits**: Postgres applies the writes in commit order, so the last write to a pad
  wins, the same outcome as Jazz's row merge for a single boolean.
- **Offline**: Realtime doesn't take writes while disconnected (the landing page copy is unchanged).
- **Audio and presence** are kept (the harness doesn't touch them). Presence is a `presence.beat`
  upsert every 5 s, without an optimistic overlay.
- Timestamps are epoch ms.

Neon-specific:

- **Writes are serialized per page.** Next.js runs a page's Server Actions one at a time, so 32
  rapid pad toggles go to the server one after another (each optimistic at once). That is the
  documented Next.js write path of Neon's example, kept as is; it shows in `wq_concurrent`.
- **No SSR of live data.** Neon's Next.js example seals on the server and passes `initialData`;
  the copied components subscribe from the client instead (as the Zero and Convex ports do), so
  the session page renders after the first live results.

## Seed

`pnpm seed` (or `seed()`) drops and recreates Wequencer's tables on the branch, signs the three
fixture users up through Managed Better Auth's HTTP API (or signs them in, when an earlier seed
created them), creates their profiles with the app's `ensureProfile`, then bulk-inserts 100
sessions (3 members, 1 pattern, 16 tracks, the fixture's pads and one transport observation each)
straight into Postgres, as the Zero seed does. Returns `{ sessionId, onPads, users }`; `signIn`
posts to the app's `/api/auth/sign-in/email` (Managed Better Auth through the app's handler).

## Smoke run (2026-11-14, `--runs 1 --samples 3`, not a canonical result)

open 1110 ms, reload 824, toggle 14, cross-client 61, concurrent 1198; 463 KB gzip loaded; rtt
2.9 ms (`/api/health`, a `SELECT 1` on Neon through the Next server); 0 lost samples.

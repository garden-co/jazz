# BigLabel on Convex: notes

## Versions

- `convex` 1.50.0, local backend `precompiled-2026-10-22-1447815`
- `@convex-dev/auth` 0.0.98 with `@auth/core` 0.37.4, **Password** provider, through
  Convex Auth's Next.js integration: `ConvexAuthNextjsServerProvider`,
  `ConvexAuthNextjsProvider`, and `convexAuthNextjsMiddleware` in `proxy.ts`. Why not
  2.0.0: see `stage-plan/convex/NOTES.md`.
- Next.js 16.2.11, React 19.2.4, Astryx 0.6.3 with the `garden-co/design` Jazz theme, as
  the Jazz copy

## Backend and hooks

A local anonymous deployment (`CONVEX_AGENT_MODE=anonymous npx convex dev`, no account),
with data in `.convex/`. `harness.mjs` works from a fresh checkout: it runs `pnpm install`
if needed, configures the deployment and auth keys, starts `convex dev`, seeds, runs
`next build` and serves the app with `next start` on :4405. `rttUrl` is the backend's
`/version`.

- **Variants**: one. Nothing is optimistic (see below).
- **`signIn(page, key)`** posts `auth:signIn` to Convex Auth's Next.js route
  (`/api/auth`), which sets the session cookies. It's untimed.
- **Logs**: `HARNESS_CONVEX_LOGS=always` writes the backend's function logs to
  `.convex/dev.log`.

**Seed** (`scripts/seed.mjs`, `pnpm seed` with `pnpm backend` running): it mirrors
`big-label/jazz/seed.ts`.
1. `seed:reset` wipes every table.
2. All 48 fixture members sign up through the Password provider and run the app's
   bootstrap mutation: profile, sign-in email and personal label. Two at a time, because
   Convex Auth hashes passwords in the function runtime and 8 at once ran into the 1 s
   limit.
3. Each label and its admin's membership is created by the server (internal
   `seed:createLabel`), as the demo-data mutation does.
4. The admin adds the other members with `members.addByEmail`.
5. Everything else is written as the label's admin through the app's public mutations,
   under the same checks: teams, catalogues, artists, team members, releases (through
   `releases.save`, with its catalogue-number check) and release teams.

It takes about 2 minutes here, mostly the 48 password hashes and about 9,800 mutation
calls.

## How the port maps

- **Permissions**: the Jazz `permissions.ts` rules are checks in every function, in
  `convex/lib/access.ts`, with the role table in the copied `src/roles.ts`.
  - Members read their label, and outsiders are denied. People are only returned through
    a label's member list, so they're visible to co-members only.
  - Admins manage members, roles, teams, catalogues and the label's name, and delete
    records.
  - Editors add and edit artists and releases and staff releases with teams.
  - Viewers read.
  - Cross-tenant references are refused: release → artist/catalogue, team assignment →
    team/membership, release team → release/team.

  I checked all of this with a script against the live backend: outsider reads,
  editor/viewer writes, self-promotion, cross-label ids, duplicate and concurrent
  catalogue numbers, admission statuses, and idempotent bootstrap and demo data.
- **Admission**: the Jazz app's server routes become mutations that decide for the
  signed-in caller only.
  - `bootstrap.ensurePersonalOrganization` (was `POST /api/bootstrap`) gets or creates
    the caller's profile, `personal-<userId>` label, admin membership and sign-in email,
    in one transaction. Duplicates are an error.
  - `demo.load` (was `POST /api/demo-data`) creates demo labels with the caller as admin.
    It's idempotent by slug; the fixture's other members are synthetic `users` rows that
    can't sign in.
  - `members.addByEmail` (was `POST /api/members`) answers non-admins "forbidden" before
    any lookup. It only reads `personEmails` inside the function, refuses the admin role,
    and reads the existing membership in the same transaction, so a double submit adds
    the person once. The statuses are the same: added, already-member, not-found,
    forbidden.
  - No query ever returns `personEmails`.

  Browsers can't create labels or people any other way.
- **Unique catalogue numbers**: `releases.save` is the Jazz app's `saveRelease`. It reads
  the number's holders through the `(organizationId, catalogNumber)` index and inserts or
  patches in the same mutation. Convex mutations are serializable transactions, so a
  concurrent save of the same number is retried against the committed row and refused.
  The check is exact, not an approximation (two concurrent saves: one committed, one
  refused). The release dialog shows "… is already in use" from that refusal. Jazz checks
  a local read first; here the server answers.
- **Query shapes**:
  - **Paging**: the same page-number UI and `limit 21 offset n`. Each sortable column has
    an index (`by_org_<column>`), and the query walks it in the chosen direction, skips
    `offset` matching rows and returns 21.
  - **Search**: `contains` on the lower-cased `searchKey`, as a filter while walking the
    index. Convex search indexes match tokens and order by relevance, so they wouldn't
    give the same rows.
  - **Ties**: they fall back to the index's `_creationTime` order, not `id`. Release dates
    are unique per label, so the default sort the harness uses is unaffected.
  - **Overview counts**: bounded reads (`take(5000)`) counted on the server, which
    returns the counts rather than the rows. The Teams page does the same with
    `take(2000)`.
- **Deletes with cascades** (release, team, member) and "artist has releases" are single
  mutations.
- **Ids and dates**: `_id` replaces `id` in the copied components, and `releaseDate` is a
  ms number.

## Optimistic updates

None. Convex's docs show optimistic updates for a counter and a chat send. Saving a
release isn't either, and its uniqueness check is decided on the server. So
`bl_update_status` and `bl_cross_client` include the mutation round trip, while dialogs
close once the server answers. Jazz applies the write locally first.

## Approximations

- **`releaseAssignments`** (per-person release roles) isn't ported. The Jazz app has the
  table and its policies but no UI or fixture rows writing it.
- **Display names**: Jazz uses Better Auth's name. Here the bootstrap reads the name and
  email from the Convex Auth `users` row.
- **Console noise**: the browser console logs a rejected `releases.save` when a number is
  taken. The Convex client logs every failed mutation.

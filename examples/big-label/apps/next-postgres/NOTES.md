# BigLabel on plain Postgres with hand-written sync

Same stack as `wequencer/postgres`: Next.js, Better Auth on Postgres, a Hono app in a route
handler for the API, writes that `pg_notify` inside their transaction, and a small ws process
(`live-server.ts`) that `LISTEN`s and tells subscribers which tables changed. The client is
TanStack Query, refetching what an event names.

## Versions

- Postgres 18 (apt), local. Next.js 16.2.11, Better Auth 1.7.22 (as the Jazz copy), tables in
  the `auth` schema from its own migrations.
- Hono 4.10 (`hono/vercel` in `app/api/[[...route]]`), `pg` 8.16, `ws` 8.23, zod 4.
- React 19.2.4, TanStack Query 5.107, Astryx 0.6.3 (the same UI packages as the Jazz copy).

Machine setup: `../../../stage-plan/apps/react-postgres/NOTES.md`. `harness.mjs` does the rest, starting from
`pnpm install`.

## Layout

- `db/001_init.sql` + `schema.ts` (row types, and the tables/relations the read endpoint may
  follow): schema.
- `src/lib/query.server.ts`: **one generic read endpoint**, `POST /api/query`.
  - Every screen asks for the same shape: a label's rows, filtered, searched, sorted, paged,
    with a related row included. So the API takes `{ table, where, orderBy, include, select,
    limit, offset }`, whitelisted against `schema.ts`, and compiles it to one SQL query.
  - Every table, and every included row, is filtered to the caller's labels. People are
    readable by themselves and their co-members.
  - Sign-in emails aren't in `schema.ts`, so no query can reach them.
  - This is the endpoint a team writes once instead of ~20 bespoke list endpoints.
- `src/lib/writes.server.ts`: `POST /api/rpc/<name>`.
  - Generic single-row `insert`, `update` and `delete`, checked per table against the README's
    roles (the same table of rules as the Jazz app's `permissions.ts`).
  - `saveRelease` (serializable).
  - Each write NOTIFYs `org:<id>` and, for membership and label changes, the people's `me`.
- `src/lib/permissions.server.ts`: role lookups and asserts. `src/roles.ts` is unchanged.
- `src/lib/bootstrap.ts`, `members.ts`, `demo-data.ts`: the app's trusted server paths, in SQL.
  Their routes are unchanged.
- `src/data/`: the client.
  - `query.ts`: a query builder with the call shapes the copied screens use
    (`app.releases.where(...).orderBy(...).include(...).limit(...)`).
  - `react.tsx`: `useAll`, `useOne`, `useDb`, `useSession`, `DataProvider`.
  - `live.ts`, `socket.ts`, `api.ts`: as in the other ports.
  - So the screens changed only their imports. `app/page.tsx` swaps `JazzProvider` for
    `DataProvider`.

## The hard parts

- **Unique catalogue numbers**: a `UNIQUE ("organizationId", "catalogNumber")` constraint.
  `saveRelease` is still a `SERIALIZABLE` transaction that reads the number's holders first,
  to answer "already in use" clearly, and it retries on 40001. Unlike Jazz, the constraint
  also covers writes that bypass `saveRelease`.
- **Admission**:
  - Labels, people and first admins are written only by the bootstrap and demo-data server
    paths. The generic `insert` refuses `organizations` and `people`.
  - Members are added by admins through `POST /api/members`, which answers non-admins 403
    before looking anything up.
  - The role must be editor or viewer, and a unique (label, person) constraint makes a double
    submit add them once.
  - Admins can change roles later. Nobody can change their own role unless they are an admin.
- **Cross-tenant references**: composite foreign keys
  (`("organizationId", "artistId") REFERENCES artists ("organizationId", id)`, and the same for
  catalogue, team, membership and release). A reference into another label fails in Postgres
  and is answered 403.
- **Deletes**:
  - `ON DELETE CASCADE` removes a release's, team's or member's assignments.
  - `ON DELETE RESTRICT` refuses deleting an artist with releases (409). Jazz did this with a
    serializable read.
- **Query shapes**: search is `strpos("searchKey", <lower-cased text>) > 0`. Paging is
  `ORDER BY <sort>, id LIMIT 21 OFFSET n`. The Overview's counts are bounded reads (limit 5000),
  as in the Jazz app.

## Optimistic updates

None. The release form is a dialog that saves to the server, the way a typical CRUD app does
it. Writes show when the server's change event triggers a refetch.

## Behaviour differences and approximations

- A row's `organizationId` can't be changed by an update (Jazz allowed it for admins of both
  labels; the UI never does it).
- `bootstrap-state.ts` is gone. The bootstrap is `INSERT ... ON CONFLICT` on unique `userId`,
  `slug` and (label, person), so concurrent bootstraps converge without a serializable retry.
- No offline mode, and no local-first variant: the copy's "live on every device" text is
  unchanged.
- The Next server logs `ECONNRESET` when TanStack cancels a superseded refetch. This is
  harmless.

## Seed

`scripts/seed.ts` follows `big-label/jazz/seed.ts`:
- All 48 users sign up through Better Auth and run `ensurePersonalOrganization`.
- Each label and its admin are written by the server, as `/api/demo-data` does.
- Its other members are added as `POST /api/members` does (`findPersonByEmail`, then
  `addMember` as the admin).
- Teams, catalogues, artists, team members, releases and release teams go through the write
  handlers as the label's admin, one transaction per label, so role checks and tenant keys
  apply.

It takes about 12 s.

## Permission checks

`BETTER_AUTH_SECRET=… NEXT_PUBLIC_APP_ORIGIN=… pnpm exec tsx scripts/check-permissions.ts`
(after `pnpm seed`, with `pnpm start` on :4341/:4342) runs 21 checks over HTTP:
- as fixture users a (admin), b (editor), l0m3 (viewer) and l1m1 (another label);
- covering tenant reads, emails, people visibility, roles, unique numbers, cross-tenant
  references, the artist delete, admission, self-promotion and removal.

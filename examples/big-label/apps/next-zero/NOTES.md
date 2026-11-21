# BigLabel on Rocicorp Zero

## Versions

- `@rocicorp/zero` 1.12.0, `zero-cache` from the same package (`NODE_ENV=production`)
- Postgres 18.6 (apt), `wal_level = logical`, local
- Next.js 16.2.11, Better Auth 1.7.22 (as the Jazz copy) on Postgres through a `pg.Pool`, its tables in
  the `auth` schema (Better Auth's own `getMigrations().runMigrations()`), not synced
- React 19.2.4, Astryx 0.6.3 with the `garden-co/design` Jazz theme (unchanged), zod 4

Machine setup (Postgres via apt, `wal_level`, the `postgres` password, Playwright on Ubuntu 26.04):
see `../../../stage-plan/apps/react-zero/NOTES.md`, "Machine setup". `harness.mjs` does the rest, starting from
`pnpm install`.

## Layout

- `db/001_init.sql` + `schema.ts`: Postgres and Zero schemas (the Jazz column names are kept)
- `queries.ts`: named queries; the read rules are their filters, resolved server-side. Every tenant
  table has a `tenantMembers` relationship (its label's memberships), and a row is visible when the
  caller is one of them
- `mutators.ts`: custom mutators; the write rules (`src/roles.ts`, unchanged) are checks in them that
  run on the server
- `app/api/zero/{query,mutate}/route.ts`: Zero's endpoints as Next route handlers;
  `app/api/{bootstrap,members,demo-data}` are the Jazz copy's server routes, ported to SQL / server
  mutators
- `app/page.tsx` reads the Better Auth session on the server; `app/big-label.tsx` is the Jazz copy's
  page (sign-in form, bootstrap) with `ZeroProvider` in place of `JazzProvider`
- `src/lib/mutations.ts` keeps the Jazz helpers' names (`saveRelease`, `deleteArtist`, …) and calls
  the mutators; the screens swap `useAll`/`useOne`/`db.*` for `useQuery`/`zero.mutate`

## The hard parts

- **Unique catalogue numbers.** `releases.save` (the Edit/Add release dialog, used by
  `bl_update_status` and `bl_cross_client`) reads the number's current holders in its server
  transaction and refuses a taken number, and Postgres backs it up with
  `UNIQUE ("organizationId", "catalogNumber")`. A concurrent duplicate fails at the index, so numbers
  stay unique for every write, not only writes made through the app. This is stronger than Jazz.
  The dialog's instant check reads local data, as in Jazz.
- **Artist delete** is refused while the artist has releases: the mutator checks, and the foreign key
  is `ON DELETE RESTRICT`.
- **Admission.**
  - No mutator creates people, labels or admins.
  - The bootstrap route (`ensurePersonalOrganization`) runs one serializable Postgres transaction
    per call. It gets or creates the profile, the `personal-<user id>` label and the admin
    membership, and retries on a serialization failure. Unique constraints on `people.userId`,
    `organizations.slug` and `memberships(organizationId, personId)` refuse duplicates.
  - Sign-in emails live in `private."personEmails"`. Zero doesn't replicate that schema, so no query
    can reach them.
  - `POST /api/members` answers non-admins with "forbidden" before looking anything up. It resolves
    the email with SQL, then runs the `memberships.add` mutator as the caller in a server
    transaction. The mutator allows admins only, editor or viewer roles only, and a `userId` that
    must be the person's.
  - People are synced only as the `person` of memberships the caller can read, so only co-members
    see them.
- **Cross-tenant references.** The mutators check, on the server, that a release's artist and
  catalogue, an assignment's team and membership, and a release team's release and team are all in
  the row's label.
- **Query shapes.**
  - Search is `searchKey LIKE '%term%'` on the lower-cased key (ZQL has no `contains`).
  - The Overview counts are bounded reads (`limit 5000`).
  - **Paging is cursor-based**: ZQL has `start(row)` but no `offset`. Each page asks for 21 rows
    ordered by `<sort>, id` and starts after the previous page's last row. `useListControls`
    remembers where each visited page starts. The pager only offers previous and next (`hasMore`),
    so this is the same UI and the same rows.

## Behaviour differences

- **Writes**: Zero applies mutations at once on the client and refuses writes while disconnected. A
  server refusal reverts the write and shows the copied app's error toast.
- **Thin and local-first variants**: the Jazz variants have no Zero equivalent. The Zero client always
  keeps its synced data in IndexedDB, so there is one variant (`default`).
- `releaseDate` is epoch ms. The screens format it the same way.
- Copy text is unchanged, including the meta description ("… on Jazz").

## Seed

`pnpm seed` (or `seed()`) resets `big_label_zero`, runs the migrations and writes the fixture the app's
own way, as `big-label/jazz/seed.ts` does:
- every member (48) signs up through Better Auth's server API and runs the bootstrap;
- each label and its first admin are created with server authority, as the demo-data route does;
- the label's admin adds the other members through the same code as `POST /api/members` (email
  lookup, then `memberships.add` as the admin);
- teams, catalogues, artists, team memberships, releases (through `releases.save`, with its
  catalogue-number check) and release teams go through the mutators as the admin, in one server
  transaction per label.

About 20 s locally. Returns `{ labelId, secondLabelId, renameReleaseId, statusReleaseId, users }`.

## Permission checks

`pnpm exec tsx scripts/check-permissions.ts` (after `pnpm seed`) runs the server mutators, queries and
the add-member path as outsiders, viewers, editors and admins against the seeded database. It covers:
- foreign labels read as empty;
- viewers can't write;
- editors can't manage teams, members or roles, delete records or promote themselves;
- admins can't add admins or a person under another account;
- no cross-tenant artist or team references;
- catalogue numbers are unique per label;
- an artist with releases can't be deleted;
- adding a member twice adds them once;
- emails are not in the Zero schema.

It writes test rows, so seed again before a benchmark run.

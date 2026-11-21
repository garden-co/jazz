# BigLabel on Jazz: comparison notes

Copied from `examples/big-label` at jazz main `5496e88` (no `apps/` variants: the example is one Next.js app),
on published `jazz-tools@2.0.0-alpha.62` / `jazz-napi@2.0.0-alpha.62`, Next 16.2.11, Better Auth 1.7.22,
Astryx 0.6.3 with the `garden-co/design` Jazz theme. The Rust `benchmarks/` are left out (not part of the app).

## Changes from upstream

- `package.json`: concrete versions instead of `workspace:`/`catalog:`; `tsx` for the seed.
- `pnpm-workspace.yaml`: `minimumReleaseAgeExclude` for the alpha.62 packages.
- `tsconfig.json`: excludes `seed.ts` and `harness.mjs`.
- `app/page.tsx`: the provider gets `client: () => import("jazz-tools/local-first")`, and the harness flags
  `NEXT_PUBLIC_JAZZ_THIN=1` (drop it) and `NEXT_PUBLIC_JAZZ_LOCAL_FIRST=now`. Upstream BigLabel ships **thin only**
  (no `client`), so here:
  - `default`: thin first, local-first in the background: what the other Jazz heroes ship, so the matrix columns
    mean the same thing for every hero. *Not* BigLabel upstream as shipped.
  - `now`: `localFirst: "now"`.
  - `thin`: BigLabel upstream as shipped.

Nothing else in the UI or data layer changed.

## Seed (`seed.ts`)

The app's own paths, as far as they go:
- every fixture member (48 users) signs up through Better Auth (`auth.api.signUpEmail`) and runs the bootstrap
  route's `ensurePersonalOrganization` (profile, sign-in email, personal label). Backend authority, as the route.
- each fixture label plus its admin's membership is written with backend authority, as `POST /api/demo-data` does:
  `organizations.allowInsert` is `never`, so no client can create one.
- the label's other members are added as `POST /api/members` does: backend email lookup (`findPersonByEmail`), then
  `addMember` as the admin through `forReqUser` (a serializable transaction; policy: admin, non-admin roles).
- teams, catalogues, artists, team memberships, releases and release teams are written as the label's admin through
  `forReqUser`, in three transactions per label (so every `exists` check sees committed rows), under `permissions.ts`.
  Releases are inserted directly, not through `saveRelease`'s serializable check: the fixture's catalogue numbers are
  unique by construction.

About 20 s on the bench machine.

## Things that make a port hard (for the port threads)

- **Unique catalogue numbers**: `saveRelease` (the Edit/Add release dialog) is a serializable transaction that reads
  the number's holders; the authority rejects it if a concurrent one changed that read. `bl_update_status` and
  `bl_cross_client` go through it. A port needs a transactional check (a Postgres unique index or serializable txn,
  a Convex mutation, Zero custom mutator, Instant: no transactions over reads, so approximate and write it down).
- **Admission**: labels and the first admin come only from server routes (bootstrap, demo data); members are added
  by admins only, by email, via `POST /api/members` (the email table is server-only, unreadable by clients), never as
  admins. People are visible only to co-members.
- **Cross-tenant references**: release → artist/catalogue, team assignment → team/membership, release team →
  release/team must be in the same label, enforced server-side.
- **Search** is a `contains` on a lower-cased `searchKey` column; **paging** is `orderBy(sort).orderBy(id)
  .limit(21).offset(n)`; the Overview counts are bounded reads (`limit 5000`), there is no count query.
- `deleteRelease` uses `db.transaction`, which the alpha.62 docs say only the local-first client has; deletes are not
  measured.

## Observations

- The browser logs a failed WebSocket to `/jazz/.../ws` (426: `next start` route handlers don't upgrade); the client
  falls back and syncs anyway. Same in the Wequencer copy.
- pnpm warns about ignored build scripts (`@astryxdesign/core`, `esbuild`, `protobufjs`, `sharp`); harmless, as in the
  other Next copies.

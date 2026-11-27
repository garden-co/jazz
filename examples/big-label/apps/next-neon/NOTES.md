# BigLabel on Neon Realtime

## Versions

- `@neon/realtime` 0.2.1 (public beta), `@neon/auth` 0.8.2 (`@neon/auth/next` client and
  `@neon/auth/next/server`), `neon` CLI 9.5.0, `@neon/config` 1.14.0
- Neon Postgres 18, project `jazz-app-comparisons` in **aws-us-east-1**, branch `big-label`, with
  Realtime and Managed Better Auth enabled through `neon.ts`
- Next.js 16.2.11, React 19.2.4, Astryx 0.6.3 with the `garden-co/design` Jazz theme (same as the Jazz
  copy), `pg` 8.16, zod 4

Setup and the shared harness adapter: see `../../../stage-plan/apps/react-neon/NOTES.md` (a Neon API key is
needed; Realtime isn't available on claimable projects). The adapter also adds
`http://127.0.0.1:4333` to Managed Better Auth's trusted domains.

## Layout (as in the Wequencer port and Neon's Next.js Realtime example)

Copied from `big-label/jazz/`: components, screens, styles, routes and copy are unchanged; only the
data calls differ (`useAll(app.x.where(…))` → `useAll(queries.x.y(args))`, `db.insert/update` →
`mutations.x(…)`), plus `app/page.tsx` reading the session on the server instead of `JazzProvider`.

- `db/001_init.sql` + `schema.ts` (row types, columns aliased to the Jazz field names)
- `src/lib/live-queries.server.ts`: the live queries, raw SQL per name, built for the signed-in user
- `app/api/live/route.ts`: seals a named query (a Route Handler: sealing and renewal are reads)
- `app/actions.ts`: the write **Server Action**; handlers in `src/lib/mutators.server.ts`
- `app/api/auth/[...path]`: Managed Better Auth's API on the app's origin
- `app/api/bootstrap`, `app/api/members`, `app/api/demo-data`: the Jazz app's server routes, on
  Postgres; `app/api/catalog-number`: the release form's next-number suggestion (a one-off read);
  `app/api/health`: the harness's rtt
- `src/queries.ts`, `src/lib/mutations.ts`: what the browser calls; `src/lib/live.ts`: the Realtime
  client, `useAll`/`useOne` and `mutate` with the optimistic overlay (Wequencer's, with `useAll`/`useOne`
  shaped like Jazz's hooks and a `committed` promise for the release form)

## Auth

Email + password through **Managed Better Auth** on the app's origin, as in the Wequencer port.
`app/page.tsx` reads the session on the server and renders the sign-in card or the app; signing in or
out reloads the page. Managed Better Auth has an organization plugin, but BigLabel keeps its own
tables (labels, memberships with roles, teams), as the Jazz app does next to Better Auth.

## Permissions

The README's roles and admission boundary, server-side. No RLS (Realtime rejects it).

- **Reads**: every query joins the caller's membership in the label (`$1` = the session's user id),
  so it stays live: a removed member stops seeing the label. People are only read through a label's
  memberships, so you see co-members only. No live query reads `person_emails`.
- **Writes**: role checks in each handler (admin / editor / viewer as in `permissions.ts`).
  Nothing in the write API creates labels or people. Members are added only by `POST /api/members`:
  the caller must be the label's admin (checked before the email lookup, and again in the insert's
  transaction), the role must be editor or viewer.
- **Cross-tenant references** are composite foreign keys on `(organization_id, id)`: release →
  artist/catalogue, team assignment → team/membership, release team → release/team. Postgres refuses
  a reference into another label whoever writes it (stronger than the Jazz policies, which only cover
  client writes). The handlers turn the violation into `permission_denied`.
- **Unique catalogue numbers**: a unique index on `(organization_id, catalog_number)`, checked inside
  `releases.save`'s transaction; of two concurrent saves of one number, one is refused ("NFC-012 is
  already in use", shown under the field). Also stronger than Jazz's: it holds for every write.
- **Deletes** use foreign keys too: deleting a release, team or member cascades to its assignments;
  an artist with releases can't be deleted (NO ACTION), as Jazz's serializable check does.
- `pnpm check-permissions` (after `pnpm seed`): 55 checks, all passing (reads per role and label, every role rule, the
  six cross-tenant references, the taken and racing catalogue numbers, admission by email). It writes test rows, so seed
  again before a benchmark run.

## Behaviour differences and approximations

- **Overview counts** are still bounded reads (at most 5,000 rows each, "5,000+" when capped), but
  counted on the server in one live query (`count(*)` over `LIMIT 5000` subqueries) instead of sending
  every row to the browser, the idiomatic SQL for the same numbers.
- **Release form**: Jazz checks the number with a local read, closes the dialog and writes. Here the
  write handler is the check, so the dialog stays open until the Server Action answers (one round trip),
  showing a taken number under the field. The page behind shows the change at once (optimistic overlay);
  a refused save drops it again.
- **Search** is `strpos(search_key, $term) > 0` (a literal `contains`; `LIKE` would treat `%`/`_` as
  wildcards). Paging is `ORDER BY <sort>, id LIMIT 21 OFFSET n`, as on Jazz.
- **Bootstrap**: get-or-create through unique constraints (`ON CONFLICT DO NOTHING`, then read) in one
  transaction, so concurrent bootstraps converge without Jazz's retry loop; a personal membership that
  isn't an admin's still fails explicitly.
- **Demo data**: the same deterministic fixture slices, bulk-inserted by the server in one transaction.
- Release dates are epoch ms (`double precision`), as in the Wequencer port.
- Text sorts use Postgres's collation (the branch's default), which may order case and punctuation
  differently from Jazz; the scenarios only sort by date.

Neon-specific (as in the Wequencer port):

- **Writes are serialized per page** (Next runs a page's Server Actions one at a time).
- **No SSR of live data**: the screens subscribe from the client.
- **Offline**: Realtime takes no writes while disconnected.

## Seed

`pnpm seed` (or `seed()`, ~60 s) drops and recreates BigLabel's tables, then follows the Jazz seed:
the 48 fixture members sign up through Managed Better Auth's HTTP API (or sign in, when an earlier
seed created them) and run the app's bootstrap (`ensurePersonalOrganization`: profile, sign-in email,
personal label); the server creates each fixture label with its first admin (as the demo-data route);
the admin adds the other members with the member route's code (`findPersonByEmail` + `addMember`);
then teams, catalogues, artists, team members, releases (through `releases.save`) and release teams
go through the write handlers as the label's admin, with their checks, in three transactions per label.

## Smoke run

2026-11-14, `--runs 1 --samples 3`, not a canonical result: open 824 ms, reload 577, search 67, next
page 107, switch label 107, update status 12, cross-client 77; 483 KB gzip loaded (523 KB in
`.next/static`); rtt 3 ms (`/api/health`, a `SELECT 1` on Neon through the Next server); 0 lost samples.

Update status is the optimistic overlay (the page shows the new status before the Server Action
answers); cross-client is the Server Action's commit plus Realtime's fan-out to the other browser.

# BigLabel

BigLabel is a synthetic, multi-tenant record-label operations app. It gives the examples-and-benchmarks program a recognizable SaaS workload: organization membership graphs, teams and roles, indexed tenant lists, artist/release relations, cold loads, and ordinary workflow churn.

Run it locally with `pnpm --dir examples/big-label dev`. It is a small Next.js
app with its own Better Auth route and JWKS endpoint. After sign-in,
`POST /api/bootstrap` uses the server-only Jazz backend secret to ensure one
personal organization and its first admin membership for the stable Better Auth
user ID. The browser then obtains a short-lived Better Auth JWT and mounts the
operations UI inside `JazzProvider`; token expiry is handled by fetching a fresh
JWT. Browser clients never receive the backend secret, and normal membership
and tenant mutations remain policy checked at the Jazz edge.

## The app

The UI is built from [Astryx](https://astryx.atmeta.com) components with the
Jazz theme from `garden-co/design`, and every page reads live Jazz queries:

- **Label switcher** at the top of the side navigation lists every organization
  the signed-in account belongs to (`memberships.where({ userId })`).
- **Overview**: big-number counts and the label's latest releases, newest first
  (the benchmark's `label_load` shape).
- **Artists** and **Releases**: compact tables with search as you type, a status
  filter, sortable columns and pagination. Each page is one bounded, ordered
  query (`orderBy(...).orderBy("id").limit(pageSize + 1).offset(...)`); the
  extra row tells the pager whether a next page exists.
- **Artist** page: the artist's releases newest first (`artist_load`).
- **Release** page: catalogue number, catalogue, format, date, status, and the
  teams working on it.
- **Catalogues**: series that group and number releases; a catalogue page pages
  through its releases (`catalog_load`).
- **Teams** and **People**: team membership, release staffing, member roles and
  a table of what each role may do.
- **Settings**: rename the label, and load demo data.

Search matches a lower-cased `searchKey` column, because `contains` is
case-sensitive.

## Roles

| Role   | Can                                                                |
| ------ | ------------------------------------------------------------------ |
| admin  | everything: members and roles, teams, catalogues, deleting records |
| editor | add and edit artists and releases, assign releases to teams        |
| viewer | read the label                                                     |

`src/roles.ts` describes the roles for the UI, which hides or disables what a
role can't do. `permissions.ts` enforces the same rules at the Jazz edge, so a
write sent anyway is refused. `tests/roles.server.test.ts` proves it against a
real local server: no self-promotion, no admin inserts, no cross-tenant team or
catalogue references, viewers can't write, editors can't manage teams or
members, and foreign labels read as empty.

Catalogue numbers are unique per label. The release form checks locally for
instant feedback, then writes in an exclusive transaction that re-checks at the
authority, so two people can't take the same number concurrently.

## Demo data

Settings → Demo data calls `POST /api/demo-data` with the `smoke` or `small`
profile. Like the bootstrap route, it verifies the caller's JWT and writes with
the backend secret: it loads `createFixture(profile)` as extra organizations
with the caller as admin, plus synthetic members, teams, catalogues, artists and
releases. Fixture IDs map to UUIDs derived from the caller, so loading twice is
a no-op and callers never share demo tenants.

## Fixtures and headless scenarios

`createFixture(profile, seed)` provides public, deterministic data for three profiles:

| Profile  | Purpose                                      |
| -------- | -------------------------------------------- |
| `smoke`  | two tenants, quick topology/E2E receipt      |
| `small`  | docs and local development                   |
| `scaled` | larger owned-slice/read-amplification checks |

`tenantOperations()` is framework-neutral. A future topology adapter should execute its declared sequence against a real client/edge/server arrangement: cold-load the membership graph, issue the indexed organization query, hydrate releases with artists, and churn release state. The authority receipt executes the tenant-isolation query against a real local edge; fixture tests do not claim to prove authorization.

Run `pnpm --dir examples/big-label test` for the deterministic fixture and isolation receipt. The workload intentionally describes current public API capabilities only; it does not duplicate `jazz-sim`'s benchmark engine.

## Coverage and planned work

- `jazz-sim s1_saas`: tenant cold load, indexed owned-slice filtering, relations/includes, write churn.
- Policy-graph fixture: organization → membership → team/assignment authorization shape.
- Future blocker: schema migrations/versioned reconnect are recorded for a shared migration/topology lane. This example does not invent a migration mechanism.

All identities, names, and IDs are generated public fixtures; no adopter data is used.

## Admission boundary

The first organization is provisioned by the app-owned, authenticated bootstrap
route—not by a client-settable JWT claim. Its exclusive transaction reads and
then gets-or-creates the external user's one profile,
`personal-<external-user-id>` organization, and admin membership as one
durable triple. A concurrent request retries its transaction against the
winner's committed triple. Any durable duplicate or mismatched personal
membership is an explicit failure, never a "first row wins" choice.

Browsers cannot insert or delete people or organizations; they may update only
their existing profile. After bootstrap, ordinary membership insertion requires
an existing admin and cannot insert the `admin` role directly, so a proposed
membership cannot grant itself authority. This bootstrap is intentionally not a
general invitation, account-linking, profile deletion, or cross-organization
membership workflow.

Production deployments must supply `BACKEND_SECRET` and `BETTER_AUTH_SECRET`
server-side. The repository's deterministic development/build fixtures are not
production fallbacks: startup fails if either production secret is absent.

The deployed authority receipt also proves cross-tenant assignments are denied
by the edge rather than hidden by a synthetic fixture.

# BigLabel

BigLabel is a synthetic, multi-tenant record-label operations app. It gives the examples-and-benchmarks program a recognizable SaaS workload: organization membership graphs, teams and roles, indexed tenant lists, artist/release relations, cold loads, and ordinary workflow churn.

Run it locally with `pnpm --dir examples/big-label dev`. It is a small Next.js
app with its own Better Auth route and JWKS endpoint. After sign-in,
`POST /api/bootstrap` uses the server-only Jazz backend secret to ensure one
personal organization and its first admin membership for the stable Better Auth
user ID. The browser then obtains a short-lived Better Auth JWT and mounts the
operations UI inside `JazzProvider`; token expiry is handled by fetching a fresh
JWT. Browser clients never receive the backend secret, and normal membership
and tenant mutations remain admin-policy checked at the Jazz edge.

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

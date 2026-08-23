# BigLabel

BigLabel is a synthetic, multi-tenant record-label operations app. It gives the examples-and-benchmarks program a recognizable SaaS workload: organization membership graphs, teams and roles, indexed tenant lists, artist/release relations, cold loads, and ordinary workflow churn.

Run it locally with `pnpm --dir examples/big-label dev`. It is a small Next.js
app with its own Better Auth route and JWKS endpoint. After sign-in,
`POST /api/bootstrap` uses the server-only Jazz backend secret to ensure one
personal organization and its first admin membership for the stable Better Auth
user ID. Browser clients never receive that secret; normal membership and
tenant mutations remain admin-policy checked at the Jazz edge.

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
route—not by a client-settable JWT claim. It retries safely by looking up the
stable `personal-<external-user-id>` slug after a conflicted exclusive write.
After bootstrap, ordinary membership insertion requires an existing admin and
cannot insert the `admin` role directly, so a proposed membership cannot grant
itself authority. Production deployments must keep `BACKEND_SECRET` and Better
Auth signing keys server-side.

The deployed cross-tenant assignment denial currently exposes a core graph
lowering failure rather than `AuthorizationDenied`; this is tracked as
`CB-012` in the repository correctness burndown. The strict policy and repro
remain in the example until that failure is fixed.

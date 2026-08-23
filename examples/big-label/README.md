# BigLabel

BigLabel is a synthetic, multi-tenant record-label operations app. It gives the examples-and-benchmarks program a recognizable SaaS workload: organization membership graphs, teams and roles, indexed tenant lists, artist/release relations, cold loads, and ordinary workflow churn.

Run it locally with `pnpm --dir examples/big-label dev`. The initial UI is deliberately useful without provisioning: it renders a deterministic public `small` fixture while the same `artists.where({ organizationId })` query is attached for a connected Jazz environment.

## Fixtures and headless scenarios

`createFixture(profile, seed)` provides public, deterministic data for three profiles:

| Profile  | Purpose                                      |
| -------- | -------------------------------------------- |
| `smoke`  | two tenants, quick topology/E2E receipt      |
| `small`  | docs and local development                   |
| `scaled` | larger owned-slice/read-amplification checks |

`tenantOperations()` is framework-neutral. A future topology adapter should execute its declared sequence against a real client/edge/server arrangement: cold-load the membership graph, issue the indexed organization query, hydrate releases with artists, and churn release state. `assertTenantIsolation()` is the minimal receipt assertion: a tenant's visible artist and release results contain no foreign organization rows.

Run `pnpm --dir examples/big-label test` for the deterministic fixture and isolation receipt. The workload intentionally describes current public API capabilities only; it does not duplicate `jazz-sim`'s benchmark engine.

## Coverage and planned work

- `jazz-sim s1_saas`: tenant cold load, indexed owned-slice filtering, relations/includes, write churn.
- Policy-graph fixture: organization → membership → team/assignment authorization shape.
- Future blocker: schema migrations/versioned reconnect are recorded for a shared migration/topology lane. This example does not invent a migration mechanism.

All identities, names, and IDs are generated public fixtures; no adopter data is used.

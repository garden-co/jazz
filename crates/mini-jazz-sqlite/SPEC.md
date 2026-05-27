# Jazz Relational Core On An Embedded Database

Status: Draft v2.

Date: 2026-05-25.

Audience: database engineers and systems engineers who do not know existing
Jazz internals.

## Reading Map

The full spec is split across focused files.

- [Orientation](spec/01-orientation.md): Reader orientation, product motivation, the running example, and top-level invariant frame.
- [Terminology](spec/02-terminology.md): The shared vocabulary for the semantic model and physical lowering.
- [Jazz Model](spec/03-jazz-model.md): The core semantic model and first write walkthrough.
- [Auth, Sessions, And Roles](spec/04-auth-sessions-roles.md): Users, sessions, trust roles, authority, and topology-facing auth semantics.
- [Product Surface](spec/05-product-surface.md): High-level API preservation goals and product operation semantics.
- [Schemas, Catalogue, And Lenses](spec/06-schemas-catalogue-lenses.md): Schema catalogue, migrations, lenses, and explicit index declarations.
- [Policies](spec/07-policies.md): SQL-lowerable policy semantics, read/write validation, and recursive policy requirements.
- [Transactions](spec/08-transactions.md): Transaction modes, outcomes, read/write sets, and local/global behavior.
- [History And Projection](spec/09-history-projection.md): Append-only row history and rebuildable current projections.
- [Visibility, Snapshots, And Branches](spec/10-visibility-branches.md): Visibility relations, historical snapshots, and branch source provenance.
- [Queries And Observed Facts](spec/11-queries-observed-facts.md): Query semantics, observed facts, aggregates, and sync-scope basis.
- [Sync And Subscriptions](spec/12-sync-subscriptions.md): Sync bundles, query settlement, subscriptions, and incoming sync application.
- [Authority And Conflicts](spec/13-authority-conflicts.md): Authority validation, dependency handling, conflict candidates, and resolution.
- [Runtime And Public Boundary](spec/14-runtime-boundary.md): Semantic system fields, runtime topology, files, errors, and wire/public boundaries.
- [Embedded Database Lowering](spec/15-embedded-database-lowering.md): SQLite-oriented physical lowering details and indexes.
- [Operations, Platform, And Tooling](spec/16-operations-platform-tooling.md): Security, export/backup, bindings, packaging, developer tooling, and admin workflows.
- [Open Areas, Strategy, And Rationale](spec/17-open-areas-strategy-rationale.md): Known undefined areas, research discipline, implementation strategy, rationale, and future revisits.
- [Invariants To Test](spec/18-invariants.md): The exhaustive invariant catalogue for implementation and whole-system tests.
- [Prototype Test Traceability](spec/19-prototype-test-traceability.md): Mapping from current prototype tests to invariant groups and remaining gaps.

## Prototype Delta: Branch Permissions And Direct Branch Queries

The mini SQLite prototype now treats branch policy as an explicit context, not
as a fallback to normal table policy.

### Design

- A schema table may declare `forBranch` read/write rules against a branch
  backing table.
- Branch backing rows are ordinary rows whose public id equals the branch id.
  Branch metadata still lives in `jazz_branch`; the backing row supplies policy
  data and ordinary row visibility.
- A branch read or write first checks that the branch backing row is readable by
  the session under the backing table's normal read policy.
- If a table declares any `forBranch` policy, branch reads/writes for that table
  must use the matching branch policy. Missing branch read/write rules deny.
- Schemas that do not declare `forBranch` policy keep the existing prototype
  behavior: branch reads and writes use the normal table policy.
- Branch policies may compare a row field to a backing-row field, matching the
  product `forBranch`/`$branch` shape.
- A branch policy may explicitly inherit the table's main read or write policy.
  Inheritance is opt-in per operation and still requires the branch backing row
  to be visible.
- Direct branch query APIs evaluate a single query against an explicit branch id
  without changing the runtime's checked-out branch.

### Invariants

- Normal reads never use `forBranch` policy.
- Branch reads for a table with `forBranch` policy never fall back to normal
  read policy.
- Branch writes for a table with `forBranch` policy never fall back to normal
  write policy.
- Missing `forBranch` read/write rules deny once that table declares any
  branch policy.
- Explicit inherited branch rules evaluate the main table policy in the branch
  view context.
- Hidden or missing branch backing rows deny branch-scoped row access.
- A branch-field policy evaluates against the backing row for the branch being
  queried or written.
- Direct branch queries return the same rows as checking out that branch and
  querying, while preserving the caller's current checkout.

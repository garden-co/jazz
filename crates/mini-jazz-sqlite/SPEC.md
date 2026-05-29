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
- [Open Areas, Strategy, And Rationale](spec/17-open-areas-strategy-rationale.md): Known undefined areas, research discipline, optimization recommendations, implementation strategy, rationale, and future revisits.
- [Invariants To Test](spec/18-invariants.md): The exhaustive invariant catalogue for implementation and whole-system tests.
- [Prototype Test Traceability](spec/19-prototype-test-traceability.md): Mapping from current prototype tests to invariant groups and remaining gaps.

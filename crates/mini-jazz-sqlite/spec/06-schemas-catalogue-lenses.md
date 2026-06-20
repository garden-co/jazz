# Schemas, Catalogue, And Lenses

## 9. Schemas, Catalogue, Migrations, And Lenses

Catalogue state tells the runtime how to interpret rows, policies, indexes,
merge strategies, and lenses.

The developer-facing project shape is:

```text
schema.ts
permissions.ts
migrations/
```

`schema.ts` defines:

- structural schema
- relations
- scalar types such as text, boolean, integer, real, timestamp, UUID, and bytes
- enums, arrays, refs, JSON schemas, defaults, and nullability
- merge strategies
- explicit `indexOnly(...)` declarations
- branch-backing table declarations
- file/blob conventions
- future confidentiality metadata

`permissions.ts` is required, even when it declares an empty explicit
permission bundle. A runtime must not infer permissive policy from a missing
permission file or bundle.

`migrations/` contains reviewed migration/lens modules between schema hashes.
Lenses belong in migrations; there is no separate top-level `lenses/` workflow.
Migration operations include column add/drop/rename, table add/drop, and table
rename. Table rename may combine with column migrations when the resulting
structure matches. Create/drop table migrations should not be mixed with column
migrations in one reviewed migration step unless the migration DSL explicitly
defines that composition.

Explicit indexes and merge strategies are part of the schema hash. If two
schema versions differ only by index declarations or merge strategy
declarations, the system should derive automatic lens compatibility because row
value shape did not change.

Physical storage layouts are not created for every catalogue/schema version.
The engine should create a new physical layout only when structural storage
shape changes. Permission-only, index-only, merge-strategy-only, and compatible
lens-only revisions may share storage while changing catalogue interpretation,
policy, indexes, or merge behavior. Pure rename migrations should be tried as
catalogue/lens changes over stable physical storage names before creating new
tables.

Catalogue publication is admin/core controlled. Edge runtimes learn catalogue
state from the global authority through a separate catalogue sync lane.
Catalogue sync is not ordinary query-scoped row sync.

Runtime work should reference a catalogue revision. A catalogue revision
contains or points to:

- structural schema definitions
- permission bundles and active permission head
- migration/lens edges
- merge strategy declarations
- explicit index declarations

Permission catalogue state is keyed by app id plus head version. The exact app
head/permission head shape remains open, but the preferred model is that normal
runtime work names a single catalogue revision rather than separately guessing
schema and permission heads.

Lenses must be SQL-lowerable in v0. An implementation may initially support
only narrow rename/project lenses.

Writes through an old schema view are copy-on-write into the current structural
layout when translation is needed:

1. read old data through lenses into the writer/current semantic view
2. apply the write in that semantic view
3. append a new history row in the selected structural layout

When the structural storage layout is unchanged, the write may append into the
same physical layout while exporting the writer's current semantic field names.
Background migration/copy-forward may optimize old layouts into newer layouts,
but correctness must not depend on eager migration.

Nullability and defaults are semantic schema features, not incidental SQLite
behavior. Omitted insert fields receive declared defaults before policy checks,
history writes, sync export, and projection rebuild. Explicit `null` on an
optional field is row content and must not be treated as omission. A not-equal
null predicate means "present optional value."

Open issues:

- exact catalogue revision/head representation
- SQL-lowerable lens IR
- schema/lens compatibility across branches
- generated index inspection workflow
- cross-schema conflict candidates and serving indexes over lens unions

Developer workflow:

- `jazz-tools validate` validates schema and permissions together
- validation emits explicit-policy diagnostics
- migration creation compares stored schema hashes and emits reviewed stubs
- migration push publishes reviewed migration/lens edges
- catalogue push publishes app-id/head-version permission bundles and heads
- dev tooling should inspect schema/lens connectivity, permission heads,
  generated indexes, and storage layout

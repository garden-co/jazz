# Schemas and permissions

Use the project's typed `schema.ts` and `permissions.ts` as the starting point. Preserve existing
schema lineage and migration conventions; do not create a migration format of your own.

- [Defining tables](https://jazz.tools/docs/schemas/defining-tables)
- [Column types](https://jazz.tools/docs/schemas/column-types)
- [Migrations](https://jazz.tools/docs/schemas/migrations)
- [Permissions](https://jazz.tools/docs/auth/permissions)
- [User-owned data recipe](https://jazz.tools/docs/recipes/access-control/user-owned-data)
- [Shared access recipe](https://jazz.tools/docs/recipes/access-control/shared-access)
- [Group permissions recipe](https://jazz.tools/docs/recipes/access-control/group-permissions)

Declare every table with `s.table(columns, relations)`, including `{}` when it has no relations.
Store IDs with `s.uuid()` and name forward relations explicitly, for example
`author: s.rel("users", "authorId")`. Declare inverse traversals explicitly with
`authoredPosts: s.reverse("posts", "author")`; the second argument names the forward relation.
There is no suffix-based inference, automatic inverse, or relation code generation.

Check the schema's declared relation names and public types before choosing query or policy-builder
syntax. A permission policy is the enforcement boundary; UI checks are not a substitute.

Do not declare `createdAt`, `createdBy`, `updatedAt`, or `updatedBy` to duplicate Jazz provenance.
Jazz supplies `$createdAt`, `$createdBy`, `$updatedAt`, and `$updatedBy` for queries, ordering, and
permissions, and stamps them on writes. Use a domain-specific name such as `publishedAt` only when
the field has distinct application semantics. Imported records that must preserve an external
system's conventional field name can use `s.allowExternalProvenanceName(...)` explicitly.

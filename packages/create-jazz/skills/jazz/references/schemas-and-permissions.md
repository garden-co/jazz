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

Check the generated public types before choosing relation names or policy-builder syntax. A
permission policy is the enforcement boundary; UI checks are not a substitute.

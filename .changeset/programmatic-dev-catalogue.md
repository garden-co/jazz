---
"jazz-tools": minor
---

Expose programmatic catalogue publication helpers from `jazz-tools/dev`.

`jazz-tools/dev` now exports `pushSchema`, `pushPermissions`, `pushMigration`, and `deploy` so tools can publish schema, permissions, migrations, and full deployments without going through the CLI. The existing `pushSchemaCatalogue` compatibility path remains available.

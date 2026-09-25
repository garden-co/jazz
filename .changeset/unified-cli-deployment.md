---
"jazz-tools": patch
---

Updated `jazz-tools deploy` to support concurrent migrations in separate branches. It now includes all missing local schema snapshots and migrations, and requires all migrations to converge in the current `schema.ts`.

Programmatic `deploy` accepts historical `schemas` and multiple `migrations`. Its result includes `changed` and `published` schemas/migrations instead of a single migration and permissions.

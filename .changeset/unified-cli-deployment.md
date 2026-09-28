---
"jazz-tools": patch
---

Updated `jazz-tools deploy` to support concurrent migrations in separate branches. It now includes all missing local schema snapshots and migrations. The target must be reachable from the server's active schema through forward migrations, or be a previously published ancestor.

Programmatic `deploy` accepts historical `schemas` and multiple `migrations`. Its result includes `changed` and `published` schemas/migrations instead of a single migration and permissions.

**BREAKING CHANGE: sync wire protocol v3 → v4.** Schema publications now support multiple incoming migrations. Clients and servers using different wire versions cannot sync, even if the app does not use concurrent migrations.

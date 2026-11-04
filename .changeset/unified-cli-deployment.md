---
"jazz-tools": patch
---

Programmatic `deploy()` (from `jazz-tools/dev` and `jazz-tools/testing`) accepts historical `schemas` and multiple `migrations`, so apps can publish concurrent migrations from separate branches. It includes all missing local schema snapshots and migrations, and its result lists the `changed` and `published` schemas and migrations instead of a single migration and permissions.

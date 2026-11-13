---
"jazz-tools": patch
---

`jazz-tools validate` no longer ends every unchanged-schema run with "Permission-only changes do not create schema hashes or require migrations."; it now says that schema.ts matches its latest snapshot and that permission changes never need a migration. `migrations create` no longer prints the note after generating a stub for a schema change, where it read as a diagnosis of that change. Thanks to @tobiaslins for reporting it (#4773).

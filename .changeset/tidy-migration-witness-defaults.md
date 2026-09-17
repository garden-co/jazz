---
"jazz-tools": patch
---

Preserve structural column defaults in generated migration witnesses, including UUID-to-relation migrations. Validate canonical witness defaults and reject unsupported default changes rather than silently producing an empty migration.

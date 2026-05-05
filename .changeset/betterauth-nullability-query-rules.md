---
"jazz-tools": patch
---

Updated BetterAuth adapter query-support rules: timestamp columns now support `ne`, `null` filters are only allowed for nullable columns, and `ne null` remains rejected for references and `id`.

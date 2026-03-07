---
"jazz-tools": patch
---

Tighten generated query helper and include types for stronger inference and stricter contracts.

This preserves include-aware returned row types by keeping `QueryBuilder<...WithIncludes<I>>` / `_rowType` aligned with selected includes, narrows generated `*Include` relation flags to `true` (instead of `boolean`), tightens `gather(...)` step callback typing, avoids optional-include selector collapse to `never` in nested array includes, and removes unnecessary `unknown` casts in generated include helpers.

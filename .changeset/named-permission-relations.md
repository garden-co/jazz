---
"jazz-tools": patch
---

Resolve permission inheritance through declared forward and reverse relationship names. `allowedTo.read/insert/update/delete` now share the relationship namespace used by `hopTo`; `*Referencing` takes a declared forward name on its source table. Raw FK column names and inferred aliases are rejected. Reject unsupported reverse `maxDepth` options instead of silently ignoring them, while preserving existing native authorization and raw policy IR semantics.

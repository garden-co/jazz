---
"jazz-tools": patch
"create-jazz": patch
---

Replace inferred relationship names with explicit table-local declarations. `s.table(columns, relations)` now requires a relationship map (use `{}` when empty). Store references as `s.uuid()` or UUID arrays, declare forward navigation with `s.rel(targetTable, column)`, and declare reverse navigation with `s.reverse(sourceTable, forwardRelationName)`.

Remove automatic reverse relationships, reference-name suffix restrictions, and pluralization. Validate declarations in TypeScript and at runtime, including cross-table targets, reserved names, and conflicting reference targets. Preserve existing core reference metadata and storage identity when migrating equivalent declarations. Update examples, starters, permissions, and migration tooling to the explicit API.

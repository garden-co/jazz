---
"jazz-tools": patch
---

Allow migrations to rename tables that have relationships, including self-references and simultaneous reference-column renames. Preserve relationship targets through explicit table renames while continuing to reject changes to a different referenced table.

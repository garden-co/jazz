---
"jazz-tools": patch
---

- A `jazz.permissions.json` policy that reads a computed field is now refused with a message saying so (`` `priority` is a computed field, which isn't stored, so a permission can't read it``), as the TS DSL's message did. That includes a relation's `outer.` column.
- `jazz.app.json` now refuses a relation over a column whose `references` names another table: before, it loaded, and the first query including the relation failed.
- `insert` now returns an omitted `bigint` column's default as a `bigint`, as reads do. Before, it was the default's decimal string.

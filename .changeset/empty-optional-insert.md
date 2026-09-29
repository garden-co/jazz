---
"jazz-tools": patch
---

Inserting a row with every optional column omitted (for example `db.insert(table, {})` on a table whose columns are all optional) now creates the row with null values instead of failing with "mergeable commits must carry content cells".

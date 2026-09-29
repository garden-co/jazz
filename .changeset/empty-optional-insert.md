---
"jazz-tools": patch
---

Inserting a row with every column omitted (for example `db.insert(table, {})`) now creates the row with null values when the table has at least one optional non-JSON column, instead of failing with "mergeable commits must carry content cells". Tables whose only columns are optional JSON still can't take such an insert; it now fails with an explicit error until optional JSON columns can store null.

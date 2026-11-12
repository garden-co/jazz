---
"jazz-tools": patch
---

Migrations get `m.add.uuid({ default })` and `m.drop.uuid({ backwardsDefault })` for a plain `s.uuid()` column. `jazz-tools migrations create` now writes them for such a column, instead of `m.ref("TODO_TABLE", ...)`, which named no real table and still validated and deployed. A column that references a table keeps `m.add.ref("table", ...)`.

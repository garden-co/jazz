---
"jazz-tools": patch
---

`jazz-tools migrations create` now refuses a schema change that keeps a column's name but changes its optionality or type, with a "not supported yet" error naming the column. Before, it wrote a migration stub that could not typecheck. A JSON column's schema and an enum column's variants count as part of its type: changing either on a kept column is refused too, since no migration step can change a column in place. Add a column under a new name and drop the old one instead.

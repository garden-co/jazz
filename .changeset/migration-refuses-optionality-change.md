---
"jazz-tools": patch
---

`jazz-tools migrations create` now refuses a schema change that keeps a column's name but changes its optionality or type, with a "not supported yet" error naming the column. Before, it wrote a migration stub that could not typecheck. A JSON column's schema and an enum column's variants count as part of its type: changing either on a kept column is refused too, since no migration step can change a column in place. To keep the values, add the new column under another name, copy the values into it from your backend, and drop the old column in a later release.

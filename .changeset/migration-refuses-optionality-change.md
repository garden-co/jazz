---
"jazz-tools": patch
---

`jazz-tools migrations create` now refuses a schema change that keeps a column's name but changes its optionality or type, with a "not supported yet" error naming the column. Before, it wrote a migration stub that could not typecheck.

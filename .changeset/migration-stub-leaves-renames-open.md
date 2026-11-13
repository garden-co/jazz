---
"jazz-tools": patch
---

`jazz-tools migrations create` no longer writes add and drop steps for a column that may have been renamed. When an added column has the same type as a removed one, the stub leaves both without a step and a TODO that names the choice: `m.renameFrom("old")` keeps the values; `m.add` and `m.drop` start the column empty. `jazz-tools validate` refuses the file until the TODO's column has a step. Before, renaming an optional column silently made every value read null after deploy.

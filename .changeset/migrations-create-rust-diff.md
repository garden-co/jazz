---
"jazz-tools": patch
---

`migrations create` now takes a stub's steps from the Rust diff (`migrationDiff` in jazz-napi) that `jazz.app.json`'s `migrations create` uses: table renames, possible column renames, defaults and refusals are decided once. Moving a kept column's reference to another table is now refused with the alternative, instead of writing a stub that a deploy refuses. A table whose rows pass unchanged gets a comment naming what changed (`// Rows pass unchanged: only branchBy changed.`). Removing a column from `branchBy` or changing a branch column's default is now refused at `migrations create`, as a deployment refuses it, instead of writing a stub no deploy accepts.

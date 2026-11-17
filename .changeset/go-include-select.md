---
"jazz-tools": patch
---

Go: `Relation.Select(cols…)` includes only some of the related rows' columns (as TS's `include({ project: app.projects.select("name") })`), and a `jazz.Select` struct may have a field for an included relation.

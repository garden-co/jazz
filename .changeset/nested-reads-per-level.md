---
"jazz-tools": patch
---

Read nested includes a level at a time. Each relation of a query now reads the children of all of that level's parent rows in one statement, with each parent's own order, offset and limit, instead of one statement per parent row. A 100-message page with nested people re-runs in a handful of statements rather than hundreds.

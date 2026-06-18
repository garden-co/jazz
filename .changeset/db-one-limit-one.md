---
"jazz-tools": patch
---

Make `db.one(...)` execute with a root query limit of one instead of fetching every matching row and discarding all but the first.

---
"jazz-tools": patch
---

Preserve fields hidden by schema lenses when an older-schema client updates a newer-schema row.

Updates now patch the original source-schema row when possible, instead of writing back a lossy transformed row that can drop columns the client could not see.

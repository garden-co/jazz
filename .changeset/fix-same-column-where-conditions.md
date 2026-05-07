---
"jazz-tools": patch
---

Fix query planning so multiple conditions on the same column are combined correctly, preserving accurate results for same-column where clauses.

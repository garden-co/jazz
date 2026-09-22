---
"jazz-tools": patch
---

Avoid full-table reads when staging browser transaction updates, and prevent failed write-merge reads from leaving patches staged for a later commit.

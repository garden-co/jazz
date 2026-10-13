---
"jazz-tools": patch
---

Range reads of large values and queries that project large columns away no longer load whole large values, so their cost no longer grows with value size.

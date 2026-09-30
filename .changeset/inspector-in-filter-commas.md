---
"jazz-tools": patch
---

Inspector `IN` / `NOT IN` filters split values only on top-level commas, so JSON values that contain commas are kept intact.

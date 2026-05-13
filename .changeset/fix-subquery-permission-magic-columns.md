---
"jazz-tools": patch
---

Fix queries with reverse relations that select permission magic columns such as `$canRead`, `$canEdit`, and `$canDelete`.

Included rows now preserve those permission values for reverse, nested, and recursive relation results instead of dropping the subquery table dependency needed to compute them.

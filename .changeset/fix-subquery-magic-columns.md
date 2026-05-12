---
"jazz-tools": patch
---

Fix reverse and nested relation includes that selected provenance magic columns such as `$createdAt` and `$updatedAt`.

Included relation rows now remain present when those magic timestamp columns are selected, instead of resolving as missing, null, or empty because subquery row descriptors dropped non-physical magic columns.

---
"jazz-tools": patch
---

Refresh filtered live queries when another row in the same table supplies their read-policy evidence. Adding or removing that supporting row now updates the visible result even when the supporting row does not match the query's root filter.

---
"jazz-tools": patch
---

An optional include whose filter or `orderBy` names a column the related table doesn't have is now refused when the query is prepared, as a required include already was, rather than at the first read that finds a parent row (or never).

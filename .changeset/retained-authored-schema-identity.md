---
"jazz-tools": patch
---

Preserve authored schema and physical table identities when validating reads across migrations. Unchanged exclusive reads can commit after a table or column rename, while concurrent matching inserts and ambiguous reused table names still reject.

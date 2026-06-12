---
"jazz-tools": patch
---

Fix `deleteClientStorage()` hanging forever when called on a persistent browser Db before any table or query has been used.

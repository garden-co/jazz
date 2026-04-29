---
"jazz-tools": patch
---

Fix MaterializeNode passing an empty table name to the row loader, which caused old-branch rows to be silently dropped after a schema migration on storage backends that resolve rows by locator. Apps with persistent local data from a previous schema would see all their old rows disappear from query results until a fresh sync.

---
"jazz-tools": patch
---

Jazz can share a SQLite file with unrelated application tables, preserving those tables when it initializes its own storage. Legacy Groove SQLite stores remain unsupported and are refused before Jazz writes to them. Recognition uses the Groove format marker in the `meta` table after checking its columns, rather than requiring an exact `CREATE TABLE` string.

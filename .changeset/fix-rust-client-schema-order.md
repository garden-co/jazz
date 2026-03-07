---
"jazz-tools": patch
---

Fix Rust schema-order compatibility when runtime table columns are sorted differently from the declared app schema, including `JazzClient` create/query flows and `SchemaManager` inserts.

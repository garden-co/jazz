---
"jazz-tools": patch
---

Fix runtime schema-order compatibility after sorted table columns.

`Db` mutations and query transforms now tolerate runtime schemas returned as `Map`s, and low-level `JazzClient` create/query/subscribe APIs preserve the declared schema column order expected by generated bindings and app code.

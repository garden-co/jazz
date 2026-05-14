---
"jazz-tools": patch
---

`Query.hopTo("relation")` now infers the destination table's row type instead of carrying the source table's type through. Consumers extracting the row type via `s.RowOf<>` (or any other consumer of the query builder's row type) now see the destination shape.

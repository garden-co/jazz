---
"jazz-tools": minor
---

Add transformed columns to the TypeScript schema DSL.

Column definers like `s.string()`, `s.boolean()`, and `s.int()` now support `.transform({ from, to })`, allowing apps to expose a transformed TypeScript value while storing the underlying column's normal SQL type. Transforms apply on rows returned from reads and subscriptions, and in reverse before inserts and updates.

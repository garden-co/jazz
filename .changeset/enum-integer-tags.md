---
"jazz-tools": patch
---

Store enum columns, and arrays of them, as small integers that each store
assigns locally. Reads, writes, filters and sync still use variant names.
Sorting by an enum column now follows the schema's declaration order instead
of alphabetical order, and adding or reordering variants makes a new schema
version.

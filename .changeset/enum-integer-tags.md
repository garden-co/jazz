---
"jazz-tools": patch
---

Store enum columns, and arrays of them, as small integers that each store
assigns locally, in the order variants were added. Reads, writes, filters and
sync still use variant names. Sorting by an enum column now follows the order
its variants were added instead of alphabetical order.

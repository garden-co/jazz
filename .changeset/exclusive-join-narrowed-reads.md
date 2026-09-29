---
"jazz-tools": patch
---

Exclusive transactions that read through a join, an include or a related list now record only the related rows they could have used. They no longer download every related row the reader can see, and a write to an unrelated row in the related table no longer makes them conflict.

Reads whose related rows can't be narrowed this way (lookup joins, flat joins, unions, includes through an array of references, or deleted rows together with related rows) now fail inside an exclusive transaction with an error such as "Reading a flat join of `invites` with `members` is not supported in exclusive transactions yet", instead of reading the whole related table.

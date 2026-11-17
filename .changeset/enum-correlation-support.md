---
"jazz-tools": patch
---

Sync support now follows rows correlated on enum columns by variant. This covers a read policy's `EXISTS`, array subqueries and junction joins, whether the enum is compared with another enum column, a text column or itself. Previously, the lookup compared the outer row's variant name with the column's stored tags and found nothing, so a relay or client could lack the rows a policy or relation rests on.

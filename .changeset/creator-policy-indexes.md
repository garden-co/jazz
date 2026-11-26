---
"jazz-tools": patch
---

Permissions that keep rows to who wrote them (`$createdBy.account = session.user`) no longer look up each row's author: they find the reader's rows through an index on `$createdBy`, which tables can now declare (`.indexOnly(["$createdBy"])`, `.compositeIndex(["$createdBy", "$createdAt"])`, or `"indexes": ["$createdBy"]` in `jazz.app.json`). With the index, a read visits only the reader's rows instead of the whole table. A backend whose permissions filter by creator on a table without such an index warns at startup, naming the table.

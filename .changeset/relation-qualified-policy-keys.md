---
"jazz-tools": patch
---

A qualified key in a policy's `where(...)` can now name one of the table's relations (`where({ "author.uid": me })`), not only the table a relation leads to. When a table name is ambiguous, or a qualifier matches nothing, the error now lists the table's relations and the keys to write instead.

**Possibly breaking:** a qualifier that is the name of a relation to one table and also the name of a table another relation leads to used to resolve as that table. It now fails with an error naming both; rename the relation or use an explicit relation. A relation named after the table it leads to (for example `todos: s.reverse("todos", ...)`) still resolves as before.

Correlating a column of a table joined past the first table of an `exists` on anything but its join key now fails with an error that names the column and says the shape isn't supported in policies yet, instead of "nested outer correlation must use its join key".

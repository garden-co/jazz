---
"jazz-tools": patch
---

A qualified key in a policy's `where(...)` can now name one of the table's relations (`where({ "author.uid": me })`), not only the table a relation leads to. When a table name is ambiguous, or a qualifier matches nothing, the error now lists the table's relations and the key to write instead. Correlating a column of a hopped-to table (`hopTo`) with the protected row on anything but the hop's key now fails with an error that names the column and says the shape isn't supported in policies yet, instead of "nested outer correlation must use its join key".

---
"jazz-tools": patch
---

`jazz-tools validate` now checks the project's migrations as a deploy would, so a migration a deploy would refuse fails `validate` instead, before it ships. The refusal names the table and each column that doesn't come out as the schema says, and how (for example `"body" comes out optional but is required`), instead of "lens operations do not reproduce target columns".

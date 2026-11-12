---
"jazz-tools": patch
---

`jazz-tools validate` now runs the checks a deploy runs on each migration, so a migration whose steps a deploy would refuse fails `validate` instead, before it ships. How the migrations join into one history is still checked at deploy, against the versions the store already runs. The refusal names the table and each column that doesn't come out as the schema says, and how (for example `"body" comes out optional but is required`), instead of "lens operations do not reproduce target columns".

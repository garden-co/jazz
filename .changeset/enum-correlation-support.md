---
"jazz-tools": patch
---

A read policy whose `EXISTS` correlates two enum columns (or an enum column with a text column) now ships every row it reads to the replicas that relay them. Each enum column stores its variants as its own tags, so the same variant could be a different stored value in the two columns, and the rows the policy consults were followed by the wrong value: a client reading through a relay could miss rows it may read.

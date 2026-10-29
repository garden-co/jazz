---
"jazz-tools": patch
---

Give every table and column a stable identity derived from the authority's
stored names, mirrored to replicas in the sync catalogue. A replica refuses
an upstream that belongs to another app, and a column that is dropped and
added back keeps its identity and values only if its type is unchanged.

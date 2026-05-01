---
"jazz-tools": patch
---

Memoize runtime schema cache keys by schema object identity so repeated writes do not reserialize the full schema on every `Db.getClient` lookup.

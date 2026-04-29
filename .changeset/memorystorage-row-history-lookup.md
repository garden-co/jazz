---
"jazz-tools": patch
---

Speed up per-row history lookups in `MemoryStorage` by nesting the history map by `row_id`, so reads scale with the row's own history rather than the table size.

---
"jazz-tools": patch
---

Add schema-level per-column merge strategies to `jazz-tools`.

Columns now default to MRCA-relative per-column LWW, and non-nullable integer columns can opt into `merge("counter")` to merge concurrent snapshots by summing their MRCA-relative deltas. Merge strategy is schema metadata, so different schema versions can resolve the same conflicting history differently without rewriting stored rows.

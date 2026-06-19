---
"jazz-tools": patch
---

Add a `merge("g-set")` strategy for non-nullable array columns. Concurrent writes converge to the grow-only union of every replica's elements, deduplicated and sorted into a canonical, byte-identical order, so an element written by one replica is never dropped by a concurrent write that never saw it.

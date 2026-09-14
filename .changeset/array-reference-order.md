---
"jazz-tools": patch
---

Preserve the source ordering and duplicate references when hydrating forward array foreign keys, instead of deduplicating and sorting the referenced rows by ID. Support reverse UUID-array correlations in array subqueries.

[PR #2865](https://github.com/garden-co/jazz/pull/2865).

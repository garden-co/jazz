---
"jazz-tools": patch
---

Add typed `notIn` query filters. Membership filters now use one canonical predicate representation for root and included relations, so `notIn` reaches the core as `Not(In(...))` rather than client-side inequality expansion.

---
"jazz-tools": patch
---

Retained hydration memo lookup and lifecycle cleanup now use a node-scoped key index, avoiding scans across unrelated cached memo entries. Memo reuse, invalidation, eviction, and byte accounting are unchanged.

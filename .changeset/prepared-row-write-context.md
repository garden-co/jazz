---
"jazz-tools": patch
---

Thread prepared row-history write contexts, index contexts, and flat row codecs through local writes so hot insert, update, and delete paths no longer infer descriptors or recompute row layouts from stored schema catalogue entries.

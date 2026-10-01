---
"jazz-tools": patch
---

Prove writes allowed by row-only write policies without hydrating every row the policy matches, so a sync server's memory no longer grows with each accepted client write.

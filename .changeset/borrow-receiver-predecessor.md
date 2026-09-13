---
"jazz-tools": patch
---

Avoid copying the complete previous supporting-fact set when receiving a subscription snapshot; only changed peer facts are cloned into the local delta.

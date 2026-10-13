---
"jazz-tools": patch
---

Writes into large ordered subscriptions no longer slow down as the result grows: snapshot positions now update incrementally instead of being rebuilt on every change.

---
"jazz-tools": patch
---

Publish ready local subscription results after storage wakes the query runtime, even when another query has already completed its work. This prevents a cold subscription from remaining loading until an unrelated write.

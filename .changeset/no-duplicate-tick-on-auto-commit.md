---
"jazz-tools": patch
---

Avoid firing a duplicate immediate tick for auto-committed direct writes. Insert, update, upsert, delete, and restore ticked both inside `commit_batch` and unconditionally afterwards; the follow-up tick now only fires when the write does not auto-seal, halving scheduler pressure during bursts of bare writes.

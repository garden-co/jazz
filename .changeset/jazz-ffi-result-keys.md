---
"jazz-tools": patch
---

jazz-ffi subscription deltas carry each result's key: `$key` on added and updated rows, and `removedKeys` beside `removed`. Live unions that match a row in several arms now apply each occurrence on its own, and history subscriptions, no longer refused, deliver each accepted version as its own result. Hosts keyed by `id` keep working.

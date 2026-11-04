---
"jazz-tools": patch
---

A blob store that fails no longer leaves clients waiting: reads and subscriptions that need its large values are refused with the store's error, and they are served again once the store answers. An S3 read denied with AccessDenied is now reported as a permission error instead of as data loss, and a large value whose leaf went missing is read again as soon as the leaf is back, with no restart.

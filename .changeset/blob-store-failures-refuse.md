---
"jazz-tools": patch
---

A blob store that fails no longer leaves clients waiting: subscriptions that need its large values are refused with the store's error, and a client's request for large-value chunks is answered once the store answers again, on the same connection. An S3 read denied with AccessDenied is now a storage error that names the missing permission, instead of being reported as data loss, and a large value whose leaf went missing is read again as soon as the leaf is back, with no restart.

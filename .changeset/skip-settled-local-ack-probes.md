---
"jazz-tools": patch
---

Avoid repeatedly reading transaction status for writes already acknowledged locally while waiting for server confirmation, improving offline bulk-write performance.

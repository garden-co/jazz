---
"jazz-tools": patch
---

Avoid synchronous transaction preflight reads that can block the host while queued database work holds the owner lock.

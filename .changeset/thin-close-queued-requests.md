---
"jazz-tools": patch
---

Reject queued thin-client requests when the runtime closes before schema binding completes, instead of leaving their reads and write waits pending.

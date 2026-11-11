---
"jazz-tools": patch
---

Reject write-receipt waits on a different database node, even when its local
sequence number matches. Receipts remain usable through schema views and after
reopening their original durable node.

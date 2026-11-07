---
"jazz-tools": patch
---

Reject attempts to commit a serializable transaction through the mergeable
commit API. A mismatched commit discards its staged writes instead of bypassing
serializable conflict checks.

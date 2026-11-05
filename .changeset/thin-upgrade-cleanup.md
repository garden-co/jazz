---
"jazz-tools": patch
---

Dispose the replacement database when an imperative thin-to-local-first upgrade fails during the old database's shutdown. Preserve the original upgrade error if replacement cleanup also fails.

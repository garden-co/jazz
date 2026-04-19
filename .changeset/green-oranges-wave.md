---
"jazz-tools": patch
---

Fix backend `forSession(...)` synced queries so session-scoped reads keep locally visible rows instead of collapsing to empty results when the connected server has no published permissions head yet.

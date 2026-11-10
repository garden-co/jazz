---
"jazz-tools": patch
---

Add core transaction handles bound to one session identity or explicit backend
attribution. Keep that identity across schema views, policy reads, staged writes
and read-only commits, and recheck session write permissions at commit. Ordinary
unbound transactions retain per-operation identity selection. Delegated replica
reads require LocalOnly until scoped remote coverage is available.

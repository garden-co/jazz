---
"jazz-tools": patch
---

Improve first sync and sequential writes by maintaining physical supporting rows directly and sending an initial snapshot followed by changes to that set.

**Sync protocol upgrade:** this release uses wire protocol v2 and cannot sync with v1 peers. Upgrade clients, native runtimes, and self-hosted Edge/Core servers together; hosted clients need a compatible server deployment.

Subscription membership and resume cursors are now kept in memory. After a process restart, remote reads obtain a fresh supporting snapshot; local-first reads can still use eligible persisted data. Existing subscription caches and cursors are discarded automatically on upgrade, while application rows, transaction history, pending local writes, and the catalogue are preserved. No manual local-storage reset is required.

Keep pending reads progressing across in-process catalogue rebuilds and cache eviction without reusing invalidated subscription answers. Local-first foreground reads continue to accept a fresh answer from their local owner while offline.

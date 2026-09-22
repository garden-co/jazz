---
"jazz-tools": patch
---

Improve first sync and sequential writes by maintaining physical supporting rows directly and sending an initial snapshot followed by changes to that set.

**Sync protocol upgrade:** this release uses wire protocol v2 and cannot sync with v1 peers. Upgrade clients, native runtimes, and self-hosted Edge/Core servers together; hosted clients need a compatible server deployment.

Subscription membership and resume cursors are now kept in memory. After a process restart, remote reads obtain a fresh supporting snapshot; local-first reads can still use eligible persisted data. Existing subscription caches and cursors are discarded automatically on upgrade, while application rows, transaction history, pending local writes, and the catalogue are preserved. No manual local-storage reset is required.

Keep pending reads progressing across in-process catalogue rebuilds and cache eviction without reusing invalidated subscription answers. Local-first foreground reads continue to accept a fresh answer from their local owner while offline.

Fix native foreground reads that could stall when a refreshed subscription needed to fetch a missing row version. Host-admitted native relays now install their session scope and request the matching authenticated upstream link, preserving isolation between sessions and support for reopening existing account databases.

Bind native permission advice to the admitted foreground identity and claims. Claims changes cancel outdated advice, and server receipt revisions are no longer confused with local claims counters.

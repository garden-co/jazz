# Multi-Tab Leader Election — TODO (MVP)

Currently only single-tab OPFS access works (exclusive `SyncAccessHandle` lock). Need leader election so multiple tabs can coordinate:

- One tab's worker owns OPFS
- Other tabs sync through the leader via BroadcastChannel or SharedWorker
- Leader failover on tab close (accept potential loss — fire-and-forget semantics)

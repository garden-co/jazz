# Storage — TODO

Remaining work for storage and platform bindings.

> Status quo: [specs/status-quo/storage.md](../status-quo/storage.md)

## Multi-Tab Leader Election

**Priority: Medium**

Currently only single-tab OPFS access works (exclusive `SyncAccessHandle` lock). Need leader election so multiple tabs can coordinate:

- One tab's worker owns OPFS
- Other tabs sync through the leader via BroadcastChannel or SharedWorker
- Leader failover on tab close (accept potential loss — fire-and-forget semantics)

## Browser E2E Verification

**Priority: Low**

A comprehensive E2E suite beyond the current 10 browser tests would exercise:

- Reload → Recovery from OPFS
- Multi-tab coordination (once leader election is done)
- Edge cases in worker bridge lifecycle

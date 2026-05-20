---
"jazz-tools": patch
---

`WorkerBridge` exports a new `LeaderMigratedError` (with stable `code: "leader-migrated"`). When the leader-tab supervisor swaps the underlying endpoint underneath a bridge — e.g. because another tab won the `navigator.locks` lease — in-flight `waitForLocalSyncFlush` / `waitForUpstreamServerConnection` calls now reject synchronously with this error instead of silently hanging until the Rust ack timeout. Calls issued after a migration also reject immediately.

The `Db` runtime calls `bridge.notifyMigrated()` on its stale bridge when the supervisor's endpoint changes; transparent retry against the freshly-attached bridge is still future work, but rejection is now deterministic and typed.

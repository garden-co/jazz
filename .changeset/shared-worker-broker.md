---
"jazz-tools": patch
"jazz-wasm": patch
---

Route persistent browser runtimes through a SharedWorker broker so tabs for the same Jazz app share one OPFS-backed leader runtime instead of each opening independent storage handles. The broker coordinates leader promotion, follower message ports, schema compatibility, visibility hints, storage resets, and failover after tab or worker crashes, preserving pending local writes while the durable path reconnects.

**Breaking change — browser support:** persistent browser mode now requires `SharedWorker`, `MessageChannel`, and Web Locks support. Browsers or embedded webviews missing those capabilities will reject `createDb()`/`createJazzClient()` startup for persistent storage instead of using the previous BroadcastChannel tab-election path. Use a supported browser runtime for persistent local storage, or switch to the memory driver with a `serverUrl` in unsupported environments.

---
"jazz-tools": patch
---

Fix two regressions in the leader-tab handoff path that could cause a follower tab to silently bypass the leader's worker.

- `Db.getClient()` previously derived `hasWorker` and `useBinaryEncoding` from `this.workerEndpoint`, which is transiently `null` between leader handoffs. A `getClient()` call landing in that window would mint a non-worker client, cache it under the schema key, and (if a `serverUrl` was configured) open its own direct WebSocket via `client.connectTransport()` — violating the "one upstream socket per `(appId, dbName)`" invariant the leader-tab topology guarantees. Worker-mode is now latched at construction (`usesSharedWorker`) and used to gate client creation flags and direct-transport setup, so a handoff window can no longer downgrade a SharedWorker-backed Db into a parallel direct-transport client.
- `ensureQueryReady()` would short-circuit when `workerBridge` was `null`, even in SharedWorker mode, letting a query proceed against an absent transport during a handoff. It now waits for the supervisor to publish a fresh endpoint and the bridge to re-attach, and surfaces a typed `LeaderMigratedError` after 15 s if no replacement arrives — matching the envelope used by `waitForInitialEndpoint`.

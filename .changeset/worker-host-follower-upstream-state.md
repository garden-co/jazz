---
"jazz-wasm": patch
---

Follower tabs now gate server-tier reads on the leader's actual upstream connection. Previously the leader worker answered a follower's `Init` with an unconditional `UpstreamConnected`, and posted upstream connect/disconnect transitions only to its own main thread — never to follower-tab ports. A follower's `WorkerBridge` therefore resolved `waitForUpstreamServerConnection()` immediately regardless of the leader's socket state, so `Db.ensureQueryReady()` could let a follower serve `server`-tier queries before the leader had an upstream connection, and the follower would miss later disconnects entirely.

The worker host now tracks its upstream-connection state, reports it accurately in the follower `Init` response, and fans every connect/disconnect transition out to all attached follower-tab ports alongside the existing main-thread post.

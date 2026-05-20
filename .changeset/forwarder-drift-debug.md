---
"jazz-tools": patch
---

Add opt-in diagnostic instrumentation for tracking when the
`WorkerBridge` ↔ main-thread cache delta-forwarding pipeline drifts.

Enable in the browser console _before_ a reproduction:

```js
globalThis.__JAZZ_DEBUG_FORWARDER__ = true;
```

The runtime then prefixes a small set of lifecycle events with
`[jazz-debug]` and records per-`Db` counters covering `WorkerBridge`
construction, init start/resolve/reject, shutdown, migration,
forwarder install/set/clear, supervisor state changes, bridge
attaches, subscription registrations, and delta callbacks observed.

Pull the snapshot from the active Db:

```js
jazzClient.db.jazzDebugDump();
```

If `dbAttachWorkerBridge > forwardersInstalled` after a reproduction,
a post-migration bridge has no server-payload forwarder and the
worker has nowhere to echo deltas back to the main-thread cache —
exactly the failure mode where the Inspector still shows the data
but `useAll` subscriptions appear stuck.

Always a no-op in production: when the flag is false, no strings are
formatted and no logs are emitted. Counters are still allocated per
`Db` (a single small POJO), but never mutated.

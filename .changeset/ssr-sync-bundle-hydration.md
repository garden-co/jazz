---
"jazz-tools": patch
---

SSR hydration can now ship the server's own CRDT sync bundle alongside the rendered rows. `useAll(query, { snapshot })` and `new QuerySubscription(query, { snapshot })` seed the rows for a synchronous first paint and hydrate the local store from the bundle, so the transition to live sync is flash-free rather than blanking to empty on a cold load. Build snapshots on the server with `createSnapshotBuilder`; the envelope stays opaque and versioned.

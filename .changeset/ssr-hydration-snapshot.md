---
"jazz-tools": patch
---

Adds SSR hydration: server-rendered query results can be passed to `JazzProvider` via a new `snapshot` prop and seeded into the orchestrator cache before the first paint.

- `jazz-tools/backend`: new `createSnapshotBuilder({ appId, schema, principalId? })` with `prefetch(db, query, options?, session?)` and `dehydrate()` for capturing server-side query results into a serialisable envelope.
- `jazz-tools/react` / `jazz-tools/react-core`: `JazzProvider` accepts a new optional `snapshot` prop (plus optional `schema` for envelope validation). The envelope is discarded with a dev-mode warning if `appId`, `principalId`, or `schemaFingerprint` mismatch the live client.
- `JazzProvider.schema` and `createSnapshotBuilder({ schema })` accept either the raw `WasmSchema` or your merged `app` directly — the `wasmSchema` field is unwrapped automatically.
- `SubscriptionsOrchestrator`: new `seedSnapshot(key, snapshot)` method and exported `computeQueryKey(appId, query, options)` helper so server-side code can derive the orchestrator cache key.
- Latent fix: `serializeQueryOptions` now uses a canonical (sorted-key) stringifier, so query option property order no longer affects cache identity.

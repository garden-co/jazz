---
"jazz-tools": patch
---

Add a `jazz-tools/shared` entry point exposing the framework-agnostic utilities the React, Svelte and Vue bindings use to turn a live query into a reactive, in-place-updated result set: `applyDelta`, `reconcileArray`, `RowChangeKind`, and the supporting types (`SubscriptionDelta`, `RowDelta`, `QueryBuilder`, `QueryOptions`, and the orchestrator's `SubscriptionsOrchestrator` / `CacheEntryHandle` / `UseAllState` shapes). The in-repo bindings now consume this shared surface so an external author can build their own binding (e.g. a signals-based `useAllSignal`) on the same utilities.

This is an advanced, use-at-your-own-risk surface — the internals our framework bindings are built on, surfaced for reuse. It is not covered by semver; the orchestrator/cache-entry/delta shapes may change between releases, so pin a version if you depend on it.

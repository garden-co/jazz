/**
 * `jazz-tools/shared` — framework-agnostic utilities for building a reactive,
 * in-place-updated `useAll`-style binding.
 *
 * This is the shared surface the in-repo React, Svelte and Vue bindings consume
 * to turn a live query into a reactive result set. It is exposed so binding
 * authors (e.g. a signals-based `useAllSignal`) can reuse it instead of
 * reimplementing it.
 *
 * **Support level: advanced — use at your own risk.** These are the internals
 * our own framework bindings are built on, surfaced for reuse. They are not
 * covered by semver: cache-entry/delta shapes may change between releases. If
 * you depend on them, pin a version.
 */

export { applyDelta, reconcileArray } from "../reconcile-array.js";
export { RowChangeKind, applySubscriptionDelta } from "../runtime/subscription-manager.js";
export type { RowDelta, SubscriptionDelta } from "../runtime/subscription-manager.js";
export type { CacheEntryHandle, UseAllState } from "../subscriptions-orchestrator.js";
export type { QueryBuilder, QueryOptions } from "../runtime/db.js";

---
"jazz-tools": patch
---

Fix a cluster of `useAll` / `QuerySubscription` correctness and parity bugs across the React, Svelte and Vue bindings and the shared subscriptions orchestrator.

- **Orchestrator:** New render-safe `computeKey()` / `peekState()` reads back the React rewrite. Subscription-setup failures no longer surface as unhandled promise rejections for callback-only consumers. On a session change, a settled entry is reset to `pending` and its listeners are told to clear, so a previous session's rows are dropped on logout/login rather than served until the new subscription's first delta.
- **React:** `useAll` is rewritten on `useSyncExternalStore` with a `getServerSnapshot` path. Reads are render-safe (no subscription opened during render), an inline `app.todos.where(...)` no longer does render-phase work, SSR reads a seeded snapshot synchronously without a layout-effect warning, and a pending suspense query now suspends on its real entry promise (opened during render) instead of one a suspended effect could never resolve, while a not-yet-supplied query still suspends so the boundary shows its fallback. The JWT refresh is now deduped at the client level, so a second provider or a remount cannot double-fire it, and the refresh latch times out so a hung `onJWTExpired` can no longer wedge auth and silently drop every later expiry.
- **Svelte:** `QuerySubscription` returns its unsubscribe directly from the effect (no shared mutable field to drop), drops `onDestroy` so it works inside `$effect.root` / `.svelte.ts`, always starts `current` as `undefined`.
- **Vue (BREAKING):** `useAll` now returns `{ data, error, loading }` refs instead of a bare `Ref<T[] | undefined>`, so a failed query is distinguishable from loading or empty. Adds a Suspense-compatible `useAllSuspense`. Migrate `const todos = useAll(...)` to `const { data: todos } = useAll(...)`.
- **Docs:** the `subscribeAll` JSDoc example uses `change.item` and the now-exported `RowChangeKind.Added` (the old `change.row` / `change.kind === 0` yielded `undefined`), and `SubscriptionDelta.all`'s freshly-allocated-per-delta contract is documented with a pointer to `applyDelta` / `reconcileArray`.

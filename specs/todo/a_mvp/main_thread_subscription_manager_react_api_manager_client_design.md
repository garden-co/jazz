# React API + Manager + Client Factory (Simplified Design)

## Status

Proposed.

## Scope

This document defines only:

1. `createJazzClient` public API
2. `JazzSubscriptionManager` API + behavior contract
3. React adapter API (`JazzProvider`, hooks, `useAll` semantics)

## Non-Goals

1. No runtime architecture changes
2. No worker protocol changes
3. No transport/storage/auth redesign

## 1) `createJazzClient` API

Entrypoints:

1. `import { createJazzClient } from "jazz-tools/react"` (browser)
2. `import { createJazzClient } from "jazz-tools/expo"` (react-native/expo)

Public types:

Reference implementation sketch:

Behavior:

1. Create `db` from `DbConfig`.
2. Construct `JazzSubscriptionManager` with `{ appId: config.appId }` and `db`.
3. Await `manager.init()`.
4. Return `{ manager, db, shutdown }`.
5. `shutdown()` must tear down manager resources and database resources.

Usage shape:

## 2) `JazzSubscriptionManager` API

Constructor boundary:

State model consumed by hooks:

Required public surface:

Behavioral guarantees:

1. Canonical-key dedup: same key shares one underlying `db` subscription.
2. First data delivery transitions `pending -> ready`.
3. `entry.subscribe(...)` emits `onReady` only when transitioning from pending to first ready.
4. After first ready snapshot, updates are emitted as deltas via `onDelta`.
5. Errors transition entry to `error` and are surfaced via `entry.error` and `onError`.
6. `entry.subscribe(...)` returns an unsubscribe function; calling it releases that hook's listener. Releasing the last listener eventually tears down the underlying subscription.

Implementation constraints:

1. Maintain one cache entry per query key.
2. Each cache entry tracks source query+tier, current state, listeners, and a ready promise for suspense.
3. `makeQueryKey(query, tier, snapshot?)` must register query metadata so `getCacheEntry(key)` can lazily materialize the backing subscription source and hydrate the ready promise when the snapshot is available.
4. Cleanup for idle entries may be delayed (timeout-based) but must be cancelable on re-subscribe.

Reference implementation sketch:

## 3) React Adapter API

Provider contract:

1. `JazzProvider` is context wiring only.
2. It receives an externally created `client`.
3. It does not create/init/shutdown manager or `db`.

Hook surface:

1. `useAll(query, opts?)`
2. `useDb()`
3. `useSession()`

Provider sketch:

`useAll` state actions:

`useAll` behavior:

1. Uses `useReducer` (not `useSyncExternalStore`).
2. Subscribes through manager cache entries keyed by `makeQueryKey`.
3. Runs `bindQueryReducer` in `useMemo`; that call both binds listeners and returns the current cache snapshot.
4. Must release token on unmount and query-key changes.
5. Suspense behavior: pending + `suspense: true` throws the cache entry promise; `error` throws the cache entry error.
6. Return `T[]` only when status is `ready`; otherwise return `undefined`.

Reference implementation sketch:

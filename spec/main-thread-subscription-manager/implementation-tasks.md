# Main Thread Subscription Manager - Implementation Tasks

## 1) Core API Changes

- [ ] Make `createJazzClient(config)` synchronous in both React and React Native entrypoints.
- [ ] Inside `createJazzClient`, call `createDb(config)` once and store it as `dbPromise`.
- [ ] Update `JazzClient` contract to include lazy DB access (`dbPromise` and/or a `readDb()` accessor) instead of requiring eagerly-ready `db`.
- [ ] Ensure `shutdown()` awaits `dbPromise` safely before shutdown operations, and is idempotent.

## 2) Manager Model

- [x] Add shared manager types: `UseAllState<T>`, `QueryEntryCallbacks<T>`, `CacheEntryHandle<T>`.
- [x] Implement key registration via `makeQueryKey(query, tier, snapshot?)` with canonical keying.
- [x] Implement `getCacheEntry(key)` that returns a stable entry handle with:
- [x] stable object identity per cache key (`getCacheEntry` should memoize by key)
- [x] `state`
- [x] `status`
- [x] `promise` as tracked thenable with `status`/`value`/`reason` for suspense fast-path compatibility
- [x] `error`
- [x] `subscribe(callbacks)` _(returns unsubscribe)_
- [x] Implement deduped subscription lifecycle (single underlying `db.subscribeAll` per key).
- [x] Hydrate entry state and tracked ready-promise from any `snapshot` passed via `makeQueryKey`.
- [x] Implement delayed release cleanup with cancel-on-resubscribe.
- [x] Guarantee callbacks:
- [x] `onReady` only on first ready transition
- [x] `onDelta` for subsequent updates
- [x] `onError` for failures

## 3) Suspense Requirements (React WG Async Guidance)

- [x] Make each entry promise stable by cache key; never create a fresh wrapper promise per render.
- [x] Do not unsuspend via unrelated state updates or `useSyncExternalStore`; unsuspend by resolving/rejecting the tracked entry promise.
- [x] Avoid conditional cache-read paths that skip the Suspense read on one render and use it on another for the same logical slot.
- [x] Use a promise/thenable form compatible with Suspense fast-path semantics for already-settled values (`status`/`value`/`reason` pattern or equivalent).
- [x] Ensure suspense throw behavior is consistent:
- [x] pending -> throw `entry.promise`
- [x] error -> throw `entry.error`
- [x] ready -> return data synchronously

## 4) React / React Native Adapters

- [x] Refactor providers to accept `client` (externally created) and expose context from that client.
- [x] Update `useDb()` to read from client DB access API.
- [x] Keep `useSession()` support on web (session from client/session promise).
- [x] Refactor `useAll()` to entry-handle flow:
- [x] key with `manager.makeQueryKey`
- [x] entry with `manager.getCacheEntry`
- [x] reducer initial state from `entry.state`
- [x] subscribe in `useLayoutEffect`
- [x] call unsubscribe function returned by `entry.subscribe()` on cleanup
- [x] suspense throw from entry state

## 5) Exports, Docs, and Examples

- [x] Export `createJazzClient` from `jazz-tools/react`.
- [x] Export `createJazzClient` from `jazz-tools/react-native` (and/or `jazz-tools/expo` alias).
- [ ] Update examples/docs to client-based provider usage.
- [ ] Document that DB creation is lazy and managed via stored `dbPromise`.

## 6) Tests and Validation

- [x] Add unit tests for manager cache-entry lifecycle and dedup behavior.
- [ ] Add hook tests for `useAll` ready/delta/error behavior and release scheduling.
- [ ] Add suspense behavior tests for stable promise identity and correct throw semantics.
- [x] Add tests verifying snapshot hydration sets entry state to `ready` and primes promise `status`/`value`.
- [ ] Add regression tests ensuring no `useSyncExternalStore`-based unsuspend path.
- [x] Run package typecheck/build and targeted browser/runtime tests.

## Reference

- React WG async guidance: https://github.com/reactwg/async-react/discussions/3

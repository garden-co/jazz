---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
"create-jazz": patch
---

Release the new incremental query and subscription core. Migrate `Db.subscribeAll` to `Db.subscribe` for complete current results, and React/React Native `useAll` array results to `{ data, isLoading, error }`; `useAllSuspense` continues to return rows. Replace removed `localUpdates`/`propagation` options with read-tier selection.

This alpha includes the private-session React Native relay with sealed Android/iOS artifacts, safer concurrent query admission and transaction recovery, and fixes to persistence, permissions, branch views, authentication and browser worker lifecycles. It also updates Better Auth compatibility to 1.7.1, pins generated starter source snapshots to the installed release, and verifies the packaged native runtime loaders.

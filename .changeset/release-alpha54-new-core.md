---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
"create-jazz": patch
---

Release the new incremental query and subscription core. Migrate `Db.subscribeAll` to `Db.subscribe` for complete current results, and React/React Native `useAll` array results to `{ data, isLoading, error }`; `useAllSuspense` continues to return rows. Replace removed `localUpdates`/`propagation` options with read-tier selection.

This alpha includes the private-session React Native relay with sealed Android/iOS artifacts, safer concurrent query admission and transaction recovery, and fixes to persistence, permissions, branch views, authentication and browser worker lifecycles. It also updates Better Auth compatibility to 1.7.1, pins generated starter source snapshots to the installed release, and verifies the packaged native runtime loaders.

Apps now use `createJazzSession` to configure their account and client once. Its `registerJWT`, `loginJWT`, `linkJWT`, `restoreLocalFirst`, and `logout` actions own graceful client replacement; linking happens outside contexts after pending writes have synced. React and React Native expose the same state through `JazzSessionProvider` and `useJazzSession`, with adapters for Svelte, Vue, and Solid. Low-level opaque account handles remain available. Node backends use `initial: { backendSecret }` or `becomeBackend({ backendSecret })` on the same session API; ready `client.db` supplies backend authority and immutable request scopes preserve user permissions. `$createdBy` and `$updatedBy` are non-null structured `{ account, identity: { issuer, subject } }` values; use `.account` for account ownership. SYSTEM authorship uses a reserved account and issuer with the originating node as subject. Local-first recovery uses `exportLocalFirstSecret` and `restoreLocalFirst`.

Persistent browser clients now recover locally acknowledged pending writes after an offline restart without blocking IndexedDB I/O. Subscriber admission remains ordered with evaluator work, authentication changes and peer shutdown. Direct `jazz-wasm` callers must now await `acceptSubscriber` and `acceptSubscriberWithSelfSignedProof`; the public `createDb` interface is unchanged.

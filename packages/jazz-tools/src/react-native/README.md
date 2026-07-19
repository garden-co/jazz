# React Native bindings

This directory restores the React Native binding shape against the v2 runtime.
It is compile-level scaffolding only: the React hooks, provider, client factory,
typed schema exports, and auth helper all typecheck, but persistent storage does
not run yet.

The current `createDb()` path installs `ReactNativeRuntimeSource`. When the
config uses persistent storage, it opens `UnimplementedSqliteStorageDriver`,
whose methods throw:

`React Native SQLite storage driver is not yet implemented — see src/react-native/README.md`

Open decisions for the RN owner:

- SQLite driver route: `op-sqlite`, `expo-sqlite`, or the surviving
  `crates/jazz-rn` native-module route with JSI.
- Runtime route: keep loading the WASM-backed v2 runtime in RN, or move the
  runtime boundary into `crates/jazz-rn` as a native module.
- Storage ABI: map the future RN SQLite driver onto the portable ordered-KV
  contract, including migration reporting, corruption behavior, teardown, and
  durability tests.

Useful pointers:

- Native module scaffold: `crates/jazz-rn/` (`android/`, `ios/`,
  `JazzRn.podspec`, generated RN bridge files).
- Port ledger rows: `dev/MAIN_INTEGRATION_LEDGER.md` rows for
  `f072cb04e`, `42e77fd38`, `52ec1e1b8`, `64b033b19`, and `6e65acff3`.
- Owning spec: `crates/jazz/SPEC/13_db_api.md`, open questions for binding
  storage and React Native runtime reuse.

# React Native bindings

This directory restores the React Native binding shape against the v2 runtime.
It is compile-level scaffolding only: the React hooks, provider, client factory,
typed schema exports, and auth helper all typecheck, but persistent storage does
not run yet.

The current `createDb()` path installs `ReactNativeRuntimeSource`. Persistent
configurations fail before opening any SQLite driver with:

`React Native persistent storage is not available in this alpha; memory mode is unverified scaffolding, not device-supported persistence`

The fail-fast boundary is intentional. A `ReactNativeSqliteStorageDriver`
cannot yet be installed into the v2 Rust ordered-KV runtime. Merely opening a
SQLite connection and then delegating queries to WASM would leave Jazz data in
the WASM store and falsely claim persistence. The deprecated driver interfaces
remain as a proposed storage ABI, but supplying one is rejected before
`open()` and does not opt into persistence. This rejection also applies when
`sqliteStorage` is combined with `driver: { type: "memory" }`; the option is
never silently ignored.

Explicit `driver: { type: "memory" }` currently reaches the v2 WASM runtime in
the Node/forks test harness. That regression proves TypeScript wiring only. It
has not run under Metro/Hermes on iOS or Android and must not be described as a
supported React Native runtime mode until an actual device smoke passes.

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

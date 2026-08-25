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

- SQLite driver route: the shared `jazz-native-relay` artifact owns SQLite;
  its Android/iOS wrappers must link that artifact rather than introducing a
  JavaScript storage driver.
- Runtime route: move the runtime boundary into `crates/jazz-rn` as the thin
  native relay command transport; it must not revive the removed UniFFI/JSI
  runtime.
- Storage ABI: map the future RN SQLite driver onto the portable ordered-KV
  contract, including migration reporting, corruption behavior, teardown, and
  durability tests.

Useful pointers:

- Native module scaffold: `crates/jazz-rn/` (`android/`, `ios/`,
  `JazzRn.podspec`, and the generated `JazzRelay` TurboModule contract).
- Tracking issue: [#1756](https://github.com/garden-co/jazz/issues/1756).
- Owning spec: `crates/jazz/SPEC/13_db_api.md`, open questions for binding
  storage and React Native runtime reuse.

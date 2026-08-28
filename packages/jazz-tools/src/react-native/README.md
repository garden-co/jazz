# React Native bindings

This directory restores the React Native binding shape against the v2 runtime.
The foreground JavaScript runtime is deliberately in-memory; persistent storage
and upstream relay ownership remain in the installed Android/iOS `JazzRelay`
artifact.

The current `createDb()` path installs `ReactNativeRuntimeSource`. A persistent
configuration must supply `nativeRelay`, containing only the platform-provided
opaque 32-byte capability and its command executor. JavaScript uses it solely
to exchange canonical peer frames with the native relay. It never receives a
SQLite handle/path, schema, session, token, or admission derivation.

Persistent configurations without that installed relay fail with:

`React Native persistent runtime requires the installed JazzRelay native artifact and its platform-provided opaque nativeRelay capability`

The fail-fast boundary is intentional. A `ReactNativeSqliteStorageDriver`
cannot yet be installed into the v2 Rust ordered-KV runtime. Merely opening a
SQLite connection and then delegating queries to WASM would leave Jazz data in
the WASM store and falsely claim persistence. The deprecated driver interfaces
remain as a proposed storage ABI, but supplying one is rejected before
`open()` and does not opt into persistence. This rejection also applies when
`sqliteStorage` is combined with `driver: { type: "memory" }`; the option is
never silently ignored.

Explicit `driver: { type: "memory" }` reaches the v2 WASM runtime without a
relay. Supplying `nativeRelay` in that mode is rejected rather than ignored.

Open decisions for the RN owner:

- Native artifacts must provide the version-compatible relay command ABI and
  issue the opaque capability; a rejected open is surfaced as a startup error.
- The relay adapter serializes frames and retains them on backpressure. A
  `Db.disconnect()` closes its foreground peer alias; `Db.reconnect()` opens a
  fresh alias against the same platform-owned relay.

Useful pointers:

- Native module scaffold: `crates/jazz-rn/` (`android/`, `ios/`,
  `JazzRn.podspec`, and the generated `JazzRelay` TurboModule contract).
- Tracking issue: [#1756](https://github.com/garden-co/jazz/issues/1756).
- Owning spec: `crates/jazz/SPEC/13_db_api.md`, open questions for binding
  storage and React Native runtime reuse.

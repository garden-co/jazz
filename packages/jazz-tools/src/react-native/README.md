# React Native bindings

This directory restores the React Native binding shape against the v2 runtime.
With a matching installed `jazz-rn` native artifact and a capability issued by
trusted platform admission, the normal `createDb()` path now opens an ordinary
in-memory foreground `Db` attached to the native SQLite relay. The public
schema-backed API can make local-first queries and subscriptions and perform
ordinary full-cell insert/update/upsert/delete transactions. It uses the same
byte codecs and `NativeRuntimeAdapter` as the other native hosts; it is not a
second React-Native-shaped database API.

The current `createDb()` path installs `ReactNativeRuntimeSource`. Persistent
configurations without the platform-issued `nativeRelay` capability fail before
opening any SQLite driver with:

`React Native persistent runtime requires the installed JazzRelay native artifact and its platform-provided opaque nativeRelay capability`

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

This is still an alpha boundary, not a device-support claim. Remote tiers,
historical/terminal relation reads, branch writes, custom attribution, restore,
large-value APIs, and JavaScript upstream transport configuration remain
unavailable from the foreground binding. Platform admission and Android/iOS
device receipts are the remaining shipping work.

Open decisions for the RN owner:

- SQLite driver route: the shared `jazz-native-relay` artifact owns SQLite;
  its Android/iOS wrappers must link that artifact rather than introducing a
  JavaScript storage driver.
- Runtime route: `crates/jazz-rn` owns the thin JSI foreground command
  transport; it must not revive the removed UniFFI runtime or introduce a
  second JavaScript row/query API.
- Storage ABI: map the future RN SQLite driver onto the portable ordered-KV
  contract, including migration reporting, corruption behavior, teardown, and
  durability tests.

Useful pointers:

- Native module scaffold: `crates/jazz-rn/` (`android/`, `ios/`,
  `JazzRn.podspec`, and the generated `JazzRelay` TurboModule contract).
- Tracking issue: [#1756](https://github.com/garden-co/jazz/issues/1756).
- Owning spec: `crates/jazz/SPEC/13_db_api.md`, open questions for binding
  storage and React Native runtime reuse.

Native authentication is admitted by trusted platform code. Pass only the
opaque `nativeRelay.capability` to `createDb`; the public session identity is
read back from native admission, without returning a bearer or private claims.
Caller `cookieSession` metadata cannot replace that identity. `updateAuthToken`
is rejected for attached native foregrounds. To switch accounts, trusted
platform code revokes every capability for the old scope, admits the new
scope, and supplies its new capability to a newly created Db. Revocation
invalidates old foregrounds and pending native operations. `db.logout()` closes
the UI foreground; sign out and revoke the native session separately through
your platform auth integration. Storage retention/deletion is a separate choice.

For an upstream admitted by native code, `await db.disconnect()` stops the
native socket before publishing explicit offline state. `await db.reconnect()`
restarts it with native-owned credentials. These calls work before the first
query and require no JavaScript server URL. Only explicit disconnect permits
`ReadTier.RemoteIfPossible` to fall back locally; a transient socket failure
does not grant that fallback.

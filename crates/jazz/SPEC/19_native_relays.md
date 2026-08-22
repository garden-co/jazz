# jazz — Specification · 19. Native relay hosts

## Overview

React Native, Swift, and Kotlin are hosts for the normal Jazz client and peer
protocol. They do not receive a separate local database, query evaluator,
mutation interpreter, or synchronization implementation.

A native host owns one **process-local relay** per explicit `{ app namespace,
storage namespace, auth scope }`. The relay contains a durable ordinary client
`Db` over a portable ordered-KV backend (SQLite in the first native host). Each
UI runtime gets a separate, in-memory ordinary client `Db`; it attaches to the
relay through the normal peer protocol. The relay may itself attach to an
upstream server through that same protocol.

This chapter defines the host boundary only. Chapters 3–8, 13, and 16 continue
to own transactions, permissions, sync, client API, and subscriptions.

## Details

### 19.1 Topology and ownership

```text
RN / Swift / Kotlin UI instance A ─ in-memory client Db ┐
RN / Swift / Kotlin UI instance B ─ in-memory client Db ├─ persistent native relay Db ─ upstream
                                                       └─ SQLite ordered-KV store
```

The relay is a normal non-history-complete `Db`:

- UI writes are ordinary local client commits sent to the relay.
- The relay persists them, forwards them to its own upstream when present, and
  carries fate/view updates back over the ordinary peer protocol.
- UI query and subscription semantics are the existing `Db` semantics.
- No binding may read directly from SQLite or bypass a `Db` to answer an app
  query.

The scope key has no token material. The platform wrapper derives an opaque
`auth_scope` only after authentication; tokens and claims belong to upstream
session negotiation. Logout explicitly closes the old scope and chooses either
retention or deletion through a separate, user-visible storage-lifecycle API.
No current host may reuse a relay after an auth-scope change.

`Db` and its peer connections are executor-local. A native relay therefore owns
all core values on one dedicated native owner thread. Host calls are encoded
commands with responses; JSI/JNI/Swift must never retain or dereference a Rust
`Db` handle. This is a host scheduling constraint, not new Jazz concurrency
semantics.

### 19.2 Native ABI

The shared native core publishes a monotonically versioned capability number.
The JavaScript wrapper declares the range it understands during `open`. If the
installed native component is outside that range, startup fails before opening
storage with a clear **“new native development/release build required”** error.
This makes OTA JavaScript updates safe without pretending they can update an
embedded Rust library.

The ABI stays coarse and binary:

- open/close relay scope and attach/detach UI client;
- encode/decode the same schema, row, query, error, and peer-frame contracts
  used by WASM/NAPI where they apply;
- drain/push peer protocol frames and lifecycle notifications;
- execute a compact command/event set for `Db` operations.

Host wrappers must not create an object-per-row native API. Subscription events
remain the maintained event stream from chapter 16. The RN TurboModule is one
such host; it is not part of the core crate.

### 19.3 SQLite backend contract

`jazz-storage-sqlite` is a native implementation of Groove's existing async
`OrderedKvStorage` contract. Its format is one SQLite WAL database with:

- versioned `meta` format markers;
- a stable interned-column-family catalog;
- bytewise ordered `(column_family, key)` primary keys;
- atomic `write_many`, including ordered storage deltas;
- explicit close and flush boundaries;
- reopen that adds requested column families without losing existing contents.

It validates an existing format before adoption and returns a structured storage
error for unknown families, malformed/foreign layouts, close, and SQLite
corruption. A future Durable Objects adapter implements this logical
ordered-KV contract using the DO SQLite API; it does not reuse `rusqlite` or
claim native file/WAL behavior.

### 19.4 Package and platform contract

The published `jazz-react-native` package is a standard current React Native
New Architecture TurboModule package:

- its iOS podspec vendors prebuilt XCFramework device and simulator slices;
- its Android Gradle module vendors AAR/shared-library ABI slices;
- React Native autolinking supports bare RN without Expo as a dependency;
- Expo prebuild/CNG/EAS discover that same native module through autolinking,
  without manual Podfile, Gradle, AppDelegate, or MainApplication changes;
- no config plugin is required unless a future optional feature truly needs
  application configuration.

Stock Expo Go cannot load Jazz's arbitrary native code and is unsupported.
Expo development builds retain Metro, QR loading, and Fast Refresh once the
matching native build is installed. JS-only changes do not rebuild Rust; a
native relay ABI or native package change does.

The Rust core is independent of React Native and Expo. Swift Package Manager
and Maven/Kotlin packages consume the same relay core and artifact slices.

### 19.5 Required verification ladder

1. Shared Rust contracts: ordered storage behavior, format rejection, reopen,
   deltas, flush/close, and planted negative checks.
2. Native relay contracts: two UI clients sharing one relay, distinct scopes,
   auth switch/logout, upstream reconnect, reload/reopen, and corrupted store.
3. First-party RN test app: scenarios emit structured machine-readable
   results; the app itself is not a Maestro test script.
4. Linux Blacksmith: Rust/TS contracts, Android native artifact build, Android
   emulator install/launch/result collection via `adb`.
5. macOS Blacksmith: iOS simulator build/link/install/launch/result collection
   via `simctl`.

CI caches Cargo+sccache, pnpm, Gradle, CocoaPods, and native artifacts by
toolchain + lockfile + native-source fingerprint. Native artifacts are built
independently from JS-only scenario changes.

## Implementation ledger

| Layer                     | Status                 | Verification                                                                                        | Remaining work                                                                             |
| ------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| SQLite ordered-KV         | implemented            | crate conformance: order/prefix/range, atomic unknown-CF rejection, reopen, format rejection, close | add injected crash/durability and full differential receipt                                |
| Native owner-thread relay | implemented foundation | compile contract; normal `Db` peer links for persistent relay ↔ in-memory clients                   | expose codec command surface and add black-box two-client/upstream tests                   |
| RN TurboModule/package    | planned                | n/a                                                                                                 | replace stale excluded UniFFI scaffold with generated-codegen package + prebuilt artifacts |
| Expo/bare RN app          | planned                | n/a                                                                                                 | first-party app, Android/iOS runners, cache actions                                        |

## Open questions

- The owner-thread relay currently proves the required core boundary, but the
  final command taxonomy should be extracted from NAPI/WASM codecs rather than
  copied by the first RN wrapper.
- Define the product-facing retention/deletion UX for logout before publishing a
  destructive `deleteScope` API.
- Measure SQLite versus RocksDB only after the common relay exists. SQLite is
  the first mobile/default adapter; storage choice remains hidden from the JS
  API.

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

The scope key has no token material. Trusted platform code derives an opaque
`auth_scope` only after authentication and admits the complete scope config to
the native host: auth scope, SQLite path, schema, persistent `DbIdentity`, and
validated session claims. JavaScript receives only an opaque random 256-bit
admission capability
and cannot choose or amend those values through the command codec. UI peer
identities are derived inside the host from the admitted author and a fresh
process-local node handle. Reusing a scope with a different path, schema, or
identity fails. Trusted logout revokes the capability and atomically closes all
relay/client aliases opened through it; guessed and revoked capabilities cannot
open a scope. Tokens belong to upstream session negotiation. Logout also
chooses either
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

ABI version 3 introduces host-generated opaque admission capabilities and
trusted revocation. Its postcard command contract is intentionally incompatible
with version 2, whose `open` command carried a predictable integer scope handle.

The ABI stays coarse and binary:

- open/close relay scope and attach/detach UI client;
- send and drain complete canonical peer frames for each UI client and the
  relay's upstream transport; host diagnostics expose only handle/queue state;
- encode/decode the same schema, row, query, error, and peer-frame contracts
  used by WASM/NAPI where they apply;
- drain/push peer protocol frames and lifecycle notifications;
- execute a compact command/event set for `Db` operations.

Host wrappers must not create an object-per-row native API. Subscription events
remain the maintained event stream from chapter 16. The RN TurboModule is one
such host; it is not part of the core crate.

The C host serializes all commands internally. Every directional peer queue has
both encoded-byte and message-count budgets. The transport seam returns its
typed `Backpressure` outcome for capacity exhaustion, allowing the ordinary
peer state machine to retain and retry a stateful send; diagnostics remain the
separate source of queue depth. Receive calls drain a
bounded batch, and each pump services a bounded round-robin subset of UI peers.
Callers retry after draining or scheduling another pump rather than spinning an
unbounded native turn.

### 19.3 SQLite backend contract

`jazz-storage-sqlite` is a native implementation of Groove's existing async
`OrderedKvStorage` contract. Its format is one SQLite WAL database with:

- versioned `meta` format markers;
- a stable interned-column-family catalog;
- bytewise ordered `(column_family, key)` primary keys;
- atomic `write_many` over ordinary ordered-key/value sets and deletes;
- explicit close and flush boundaries;
- reopen that adds requested column families without losing existing contents.

It validates an existing format before adoption and returns a structured storage
error for unknown families, malformed/foreign layouts, close, and SQLite
corruption. A future Durable Objects adapter implements this logical
ordered-KV contract using the DO SQLite API; it does not reuse `rusqlite` or
claim native file/WAL behavior.

### 19.4 Package and platform contract

`jazz-rn` requires the React Native New Architecture. That is an intentional
current boundary: the generated relay spec is a TurboModule, and old
architecture builds fail during Android Gradle evaluation or iOS pod install
with an actionable instruction to enable it (Expo: add the `jazz-rn` config
plugin, then run `expo prebuild`).

**Current checkpoint (not device support).** The package autolinks an Android
and iOS `JazzRelay` TurboModule. A source/package build without staged native
artifacts reports ABI `0` and rejects commands. Trusted artifact jobs build the
Android static libraries and iOS XCFramework, and the npm file contract includes
them when the release assembly stages them; merely producing a CI artifact does
not make an npm package usable. Stock Expo Go cannot load arbitrary native code
and is unsupported.

**Target shipping contract.** A published `jazz-rn` package is a standard
current React Native New Architecture TurboModule package:

- its iOS podspec vendors prebuilt XCFramework device and simulator slices;
- its Android Gradle module vendors AAR/shared-library ABI slices;
- React Native autolinking supports bare RN without Expo as a dependency;
- Expo prebuild/CNG/EAS discover that same native module through autolinking,
  without manual Podfile, Gradle, AppDelegate, or MainApplication changes. The
  current config plugin remains only to require New Architecture for Expo apps,
  not to locate the native code.

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

| Layer                     | Status                 | Verification                                                                                        | Remaining work                                                             |
| ------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| SQLite ordered-KV         | implemented            | crate conformance: order/prefix/range, atomic unknown-CF rejection, reopen, format rejection, close | add injected crash/durability and full differential receipt                |
| Native owner-thread relay | implemented foundation | lifecycle/frame host codec; normal `Db` peer links for persistent relay ↔ in-memory clients         | platform artifact wrappers and black-box two-client/upstream restart tests |
| RN TurboModule/package    | checkpoint implemented | generated Android+iOS `JazzRelay` contract, unavailable ABI/error receipts                          | embed and package prebuilt artifacts                                       |
| Expo/bare RN app          | prebuild scaffold      | New-Architecture config plugin plus Android/iOS prebuild commands                                   | first-party device app, Android/iOS runners, cache actions                 |

## Open questions

- The owner-thread relay currently proves the required core boundary, but the
  final command taxonomy should be extracted from NAPI/WASM codecs rather than
  copied by the first RN wrapper.
- Define the product-facing retention/deletion UX for logout before publishing a
  destructive `deleteScope` API.
- Measure SQLite versus RocksDB only after the common relay exists. SQLite is
  the first mobile/default adapter; storage choice remains hidden from the JS
  API.

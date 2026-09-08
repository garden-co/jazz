# jazz-rn

This directory is the native relay package for Jazz's supported React Native
alpha. Applications use the public `jazz-tools/react-native` client API: prepare
an `AccountHandle`, then open and own a `createJazzClient` instance. The
[canonical Expo scaffold](../../examples/todo-client-localfirst-expo) shows the
complete account-handle and client lifecycle.

The alpha requires a matching native development or release build. Expo Go
cannot contain this native module.

The shared JSI HostObject source plus Rust C ABI opens an account-scoped native
foreground `Db`, runs ordinary bounded core turns, and closes that exact alias.
The relay owns its persistent SQLite scope. Android and iOS install the shared
private factory through their New-Architecture JSI hooks, including a retained
host-state lease that makes late finalizers harmless during bridge teardown.
`jazz-tools/react-native` maps the shared local-first query/subscription and
ordinary full-cell transaction commands onto its existing
`NativeRuntimeAdapter`. The public account-handle path owns native admission
and the account-scoped persistent relay; applications do not construct a
foreground engine or exchange native capabilities directly. The complete
ownership/threading/packaging contract and staged acceptance path are specified in
[`jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md#196-foreground-native-runtime-execution).

The package reserves a generated `JazzRelay` TurboModule boundary. Android and
iOS autolink the module and reject relay use unless a matching development or
release assembly stages the shared Rust relay artifact. Staged Android
libraries, the shared header, and the iOS XCFramework are included by the npm
package file contract. `expo prebuild` and bare React Native discovery prove
install-time wiring; they are not a substitute for an installed-device receipt.

`jazz-rn` requires the React Native **New Architecture**. Android Gradle and
iOS CocoaPods fail early with an install/configuration instruction otherwise.
For Expo, add `"plugins": ["jazz-rn"]` and run `expo prebuild`; the plugin
sets `newArchEnabled`. Bare React Native apps must enable the New Architecture
themselves. This requirement does not make Expo Go capable of loading Jazz.

The repository runs source/package receipts plus label-gated Android-emulator
and iOS-simulator installed-device workflows. They prove native admission,
capability revocation, process reopen, scope-selected SQLite isolation, and
the public account-handle client path. They do **not** yet prove communication
between two physical JSI runtimes; that acceptance receipt remains pending.
The shared host codec now stages trusted native scope admission/revocation via
random 256-bit capabilities, client open-close, bounded `Pump`, directional
bounded peer-frame send/drain, and handle/queue diagnostics. Kotlin and
Swift/Objective-C application authentication code supplies the complete strict
scope configuration to a dedicated native entrypoint; Rust validates and
normalizes it, then returns only the opaque 32-byte capability. The generic
TurboModule `execute` channel never accepts scope configuration, claims, or
bearer tokens. On auth switch, trusted code revokes the old capability (closing
all of its relay/client aliases) before admitting the new scope.

The shared artifact seam is `jazz_native_relay_abi_version` from
`jazz-native-relay`'s C ABI (`include/jazz_native_relay.h`). Android/JNI will
link that artifact directly when the Android build pipeline exists; it must not
route through the obsolete UniFFI library. The remaining Android runner gate is
a real Gradle/NDK AAR build and emulator installation against that linked
artifact.

The wrapper accepts ABI V1, which uses opaque host-generated admission
capabilities and trusted revocation. ABI V1 defines the shared foreground
`NativeDb` postcard seam with canonical-query prepare/read/subscribe/drain,
plus pending-operation poll/cancel commands for chunk-backed reads. It is a
byte-oriented native-host contract, not a new React Native row/query API:
`jazz-tools/react-native` is the public adapter, using the same query and
row-delta codecs as NAPI/WASM. It selects the account-scoped persistent
foreground path and does not fall back to a browser-WASM or generic
TurboModule runtime.

An admitted native foreground is bound to the session used for admission.
`db.updateCookieSession(...)` therefore rejects atomically on this path: it
leaves the previously admitted session and capability usable, and does not
silently apply refreshed claims. Revoke the old capability and create a new
client for a sign-in, sign-out, or scope change.

## Expo development-build install path

`jazz-rn` is a direct application dependency: React Native codegen and Expo
autolinking must discover it from the application, not through `jazz-tools`.
For an Expo development build, the minimal configuration is:

```bash
pnpm add jazz-tools@alpha jazz-rn@alpha
```

```json
{
  "expo": {
    "plugins": ["jazz-rn"]
  }
}
```

Then run `npx expo prebuild --clean` and make a development or release build.
The plugin turns on the New Architecture, and native autolinking registers
`JazzRelayPackage` on Android and the `JazzRn` pod on iOS. Do not use Expo Go:
it cannot contain the relay module.

Expo Go is not supported. For a bare Android host set `newArchEnabled=true`;
for iOS install pods with `RCT_NEW_ARCH_ENABLED=1 bundle exec pod install`.
Use the `rn-preview-release` pull-request label when a preview must run the
expensive native artifact and device workflows. Then prepare an account through
`jazz-tools/expo` (or an OS-protected `AccountStore` in a bare host) and pass
the resulting `AccountHandle` to `createJazzClient` from
`jazz-tools/react-native`.

Bare React Native uses the same direct dependency and autolinking metadata, but
has no Expo plugin: enable the New Architecture in the host project before
running its platform install/build commands.

The repository's `jazz-rn` packaging receipt packs the actual npm tarball,
uses it from an otherwise empty Expo SDK 54 app, typechecks an import from its
published declarations, prebuilds Android and iOS, and verifies Android
autolinking plus bare React Native discovery. This intentionally proves only
install-time wiring. A device run also requires a matching release payload with
the sealed Android relay slices or iOS XCFramework plus account-handle admission;
those are produced by the native artifact/release workflows and are checked separately by the device
acceptance app. A source checkout with no staged native artifacts should fail
at relay use rather than pretend to provide persistence.

## Alpha validation limit

The installed-device receipt currently covers two foreground aliases in one
physical JSI runtime. It does not yet prove two physical JSI runtimes attached
to one relay. This remains an explicit validation gap; it does not change the
application-facing account-handle API.

Expo Go cannot load Jazz native code. Expo development builds and bare React
Native hosts must include a matching native module and open it through an
account handle.

The normative host design and its implementation ledger live in
[`crates/jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md).

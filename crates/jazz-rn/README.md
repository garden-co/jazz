# jazz-rn

This directory is the React Native package for Jazz's native relay boundary.
It is a narrow alpha rather than general React Native support. React Native is
not yet a supported high-level React Native Jazz client: a matching native
development or release build, plus a capability issued by trusted platform
admission, are necessary building blocks, but do not yet constitute an
app-facing support guarantee.
Expo Go cannot contain this native module.

The shared JSI
HostObject source plus Rust C ABI opens one memory-only foreground `Db` only
from an already admitted opaque 32-byte capability, runs ordinary bounded core
turns, and closes that exact alias. It does not accept storage paths, schema,
claims, identities, or tokens, and it never reads relay SQLite for a foreground
operation. Owner work is bounded and capability revocation makes all aliases
unusable. Android and iOS install the shared private factory through their
New-Architecture JSI hooks, including a retained host-state lease that makes
late finalizers harmless during bridge teardown. `jazz-tools/react-native` maps
the shared local-first query/subscription and ordinary full-cell transaction
commands onto its existing `NativeRuntimeAdapter`. That implementation is a
WIP capability rather than a supported app API until rebuilt Android and iOS
artifacts and real-app device E2E receipts prove the complete path. Advanced
families (remote tiers, branches, restore, large values, and trusted-serving
reads) are still deliberately unavailable. The complete
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
one-runtime foreground operation. They do **not** yet prove communication
between two physical JSI runtimes; that acceptance receipt remains pending.
The shared host codec now stages trusted native scope admission/revocation via
random 256-bit capabilities, client open-close, bounded `Pump`, directional
bounded peer-frame send/drain, and handle/queue diagnostics. Kotlin and
Swift/Objective-C application authentication code supplies the complete strict
scope configuration to a dedicated native entrypoint; Rust validates and
normalizes it, then returns only the opaque 32-byte capability. The generic
TurboModule `execute` channel never accepts scope configuration, claims, or
bearer tokens. On auth switch, trusted code revokes the old capability (closing
all of its relay/client aliases) before admitting the new scope. This is still
a platform checkpoint, not device support: there is no assembled release-package
or Expo development-build receipt yet.

The shared artifact seam is `jazz_native_relay_abi_version` from
`jazz-native-relay`'s C ABI (`include/jazz_native_relay.h`). Android/JNI will
link that artifact directly when the Android build pipeline exists; it must not
route through the obsolete UniFFI library. The remaining Android runner gate is
a real Gradle/NDK AAR build and emulator installation against that linked
artifact.

The wrapper accepts ABI 7, which uses opaque host-generated admission
capabilities and trusted revocation. ABI 7 extends V1 of the shared foreground
`NativeDb` postcard seam with canonical-query prepare/read/subscribe/drain,
plus pending-operation poll/cancel commands for chunk-backed reads. It is
deliberately a byte-oriented native-host contract, not
a new React Native row/query API: the query and row-delta codecs are the same
ones used by NAPI/WASM. This capability-gated slice supports ordinary
local-first reads plus full-cell write/transaction commands using the
established native encoded-cell and core transaction semantics, including the
public `txId` receipt identity. Remote tiers and structured relation terminal
operations remain unavailable. Branch
targets, custom write attribution, large-value diffs, structured relation
terminal operations, and remote read tiers remain unavailable.
`jazz-tools/react-native` selects this adapter for the ordinary
capability-gated persistent foreground path; it does not select a browser-WASM
or generic TurboModule runtime as a fallback.

## Expo development-build install path

`jazz-rn` is a direct application dependency: React Native codegen and Expo
autolinking must discover it from the application, not through `jazz-tools`.
For an Expo development build, the minimal configuration is:

```bash
pnpm add jazz-rn@alpha
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
Use the `react-native/rn-preview-release` pull-request label when a preview
must run the expensive native artifact and device workflows. The alpha still
does not support every Jazz operation family: remote tiers, branches, restore,
large-value diffs, custom attribution, and relation terminal operations fail
closed rather than taking a separate React-Native path.

Bare React Native uses the same direct dependency and autolinking metadata, but
has no Expo plugin: enable the New Architecture in the host project before
running its platform install/build commands.

The repository's `jazz-rn` packaging receipt packs the actual npm tarball,
uses it from an otherwise empty Expo SDK 54 app, typechecks an import from its
published declarations, prebuilds Android and iOS, and verifies Android
autolinking plus bare React Native discovery. This intentionally proves only
install-time wiring. A device run also requires a matching release payload with
the sealed Android relay slices or iOS XCFramework plus platform-owned scope admission; those are produced by
the native artifact/release workflows and are checked separately by the device
acceptance app. A source checkout with no staged native artifacts should fail
at relay use rather than pretend to provide persistence.

## What remains before React Native is generally supported

1. Prove two **physical** JSI runtimes attached to one relay on installed
   Android and iOS apps; the present receipt only covers two aliases in one JSI
   runtime.
2. Complete and test the remaining maintained `NativeDb` families without
   creating an object-per-row React Native API.
3. Turn the narrow capability-gated alpha into a documented app-facing
   authentication/admission integration.

Expo Go cannot load Jazz native code. Expo development builds and bare React
Native hosts must include a matching native module and receive admission from
trusted platform code.

The normative host design and its implementation ledger live in
[`crates/jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md).

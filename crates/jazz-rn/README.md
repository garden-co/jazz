# jazz-rn

This directory is the React Native package for Jazz's native relay boundary.
It is not yet a supported React Native Jazz client: its autolinked Android and
iOS modules deliberately report that the Rust relay artifact is unavailable.

The active restoration work is deliberately split in two:

- `jazz-storage-sqlite` implements the portable ordered-KV storage contract
  that a native host will use.
- `jazz-native-relay` establishes the process-local, scope-keyed native relay
  boundary: a durable relay `Db` and ordinary in-memory UI clients communicating
  through Jazz's normal peer protocol.

Those crates have Rust contract tests, but they are not wired through this
package yet. The former UniFFI/JSI surface has been removed rather than left as
a broken alternative runtime path.

The package reserves a generated `JazzRelay` TurboModule boundary. Android and
iOS autolink the module, report ABI `0` (unavailable), and explicitly reject
commands unless a development or release assembly stages the shared Rust relay
artifact. Staged Android libraries, the shared header, and the iOS XCFramework
are included by the npm package file contract. `expo prebuild` and bare React
Native integration can therefore succeed without an artifact, but they do
**not** make Jazz usable on a device yet.

`jazz-rn` requires the React Native **New Architecture**. Android Gradle and
iOS CocoaPods fail early with an install/configuration instruction otherwise.
For Expo, add `"plugins": ["jazz-rn"]` and run `expo prebuild`; the plugin
sets `newArchEnabled`. Bare React Native apps must enable the New Architecture
themselves. This requirement does not make Expo Go capable of loading Jazz.

The current repository gate executes Expo prebuild for Android and iOS, then
inspects Expo's autolinking contracts for this package. It is intentionally not
a native build receipt: this Linux development environment has neither Java
nor CocoaPods, so Gradle configuration/build and `pod install` must run on the
respective Blacksmith runners before claiming platform or device support.
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

The wrapper accepts ABI 3, which uses opaque host-generated admission
capabilities and trusted revocation.

## What remains before React Native is supported

1. Link the staged host lifecycle codec through thin JNI/Swift translation and
   extend it with shared event/peer-frame drainage (not an object-per-row API).
2. Build real iOS XCFramework and Android AAR/shared-library slices from that
   module.
3. Verify bare React Native autolinking plus Expo prebuild/development builds
   on Android and iOS in CI, using a first-party device app and structured
   scenario results.

Expo Go cannot load Jazz native code. Expo development builds will be supported
once the native module and artifacts above exist.

The normative host design and its implementation ledger live in
[`crates/jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md).

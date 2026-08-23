# jazz-rn

This directory is the **legacy React Native binding scaffold**. It is not a
supported React Native Jazz client and must not be published as one.

The active restoration work is deliberately split in two:

- `jazz-storage-sqlite` implements the portable ordered-KV storage contract
  that a native host will use.
- `jazz-native-relay` establishes the process-local, scope-keyed native relay
  boundary: a durable relay `Db` and ordinary in-memory UI clients communicating
  through Jazz's normal peer protocol.

Those crates have Rust contract tests, but they are not wired through this
package yet. The code under `rust/`, `src/generated/`, `ios/`, `android/`, and
`cpp/` is the prior UniFFI-generated surface. It predates the async core and is
not the planned command/codec ABI; in particular it must not be treated as an
implementation of the relay.

The package now reserves a generated `JazzRelay` TurboModule boundary. Android
registers the generated module but reports ABI `0` (unavailable) and explicitly
rejects commands until an Android build embeds the shared Rust relay artifact.
The shared host codec now stages `Open`, `Attach`, `CloseClient`, `CloseRelay`,
and bounded `Pump`; no Android artifact calls it yet. This is a thin platform
checkpoint, not device support: there is still no linked JNI artifact,
XCFramework, AAR, or Expo development-build receipt.

The shared artifact seam is `jazz_native_relay_abi_version` from
`jazz-native-relay`'s C ABI (`include/jazz_native_relay.h`). Android/JNI will
link that artifact directly when the Android build pipeline exists; it must not
route through the obsolete UniFFI library. The remaining Android runner gate is
a real Gradle/NDK AAR build and emulator installation against that linked
artifact.

## What remains before React Native is supported

1. Link the staged host lifecycle codec through thin JNI/Swift translation and
   extend it with shared event/peer-frame drainage (not an object-per-row API).
2. Replace the stale UniFFI surface and build real iOS XCFramework and Android
   AAR/shared-library slices from that module.
3. Verify bare React Native autolinking plus Expo prebuild/development builds
   on Android and iOS in CI, using a first-party device app and structured
   scenario results.

Expo Go cannot load Jazz native code. Expo development builds will be supported
once the native module and artifacts above exist.

The normative host design and its implementation ledger live in
[`crates/jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md).

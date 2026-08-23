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
commands until a development or release build embeds the shared Rust relay
artifact. `expo prebuild` and bare React Native integration can therefore
succeed without a stale native framework, but they do **not** make Jazz usable
on a device yet.
The shared host codec now stages `Open`, `Attach`, `CloseClient`, `CloseRelay`,
and bounded `Pump`; no platform artifact calls it yet. This is a thin platform
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
2. Build real iOS XCFramework and Android AAR/shared-library slices from that
   module.
3. Verify bare React Native autolinking plus Expo prebuild/development builds
   on Android and iOS in CI, using a first-party device app and structured
   scenario results.

Expo Go cannot load Jazz native code. Expo development builds will be supported
once the native module and artifacts above exist.

The normative host design and its implementation ledger live in
[`crates/jazz/SPEC/19_native_relays.md`](../jazz/SPEC/19_native_relays.md).
